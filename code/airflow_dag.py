from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import os
import json
import requests
import great_expectations


def get_access_token(client_id: str, client_secret: str, grant_type: str = "client_credentials") -> str:
    """
    Retrieves an access token from the Spotify Accounts service.

    Parameters:
    client_id (str): The client ID provided by Spotify for your application.
    client_secret (str): The client secret provided by Spotify for your application.
    grant_type (str): The type of grant being requested. Default is "client_credentials".

    Returns:
    str: A string containing the access token prefixed with "Bearer ".

    Raises:
    requests.exceptions.RequestException: If the request to the Spotify Accounts service fails.
    KeyError: If the response does not contain an access token.

    Example:
    >>> get_access_token("your_client_id", "your_client_secret")
    'Bearer BQ...'

    Note:
    Ensure that you handle exceptions when calling this function, as network issues or invalid credentials can cause it to fail.
    """
    url = "https://accounts.spotify.com/api/token?grant_type={}&client_id={}&client_secret={}".format(grant_type, client_id, client_secret)
    response = requests.post(url, headers={'Content-Type':'application/x-www-form-urlencoded'})
    access_token = "Bearer " + json.loads(response.text)["access_token"]

    return access_token

def get_data(url: str, access_token: str) -> dict:
    """
    Retrieves data from a specified URL using the provided access token for authorization.

    Parameters:
    url (str): The URL from which to retrieve data.
    access_token (str): The access token to be used for authorization in the request header.

    Returns:
    dict: A dictionary containing the JSON response from the URL.

    Raises:
    requests.exceptions.RequestException: If the request to the URL fails.
    json.JSONDecodeError: If the response is not valid JSON.
    ValueError: If the response does not contain the expected data.

    Example:
    >>> get_data("https://api.spotify.com/v1/me", "Bearer BQ...")
    {'id': 'user_id', 'display_name': 'User Name', ...}
    """
    try:
        response = requests.get(url, headers={'Authorization': access_token})
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        result = response.json()  # Parse JSON response
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON response: {e}")
        raise
    except ValueError as e:
        print(f"Unexpected data format: {e}")
        raise

    return result


import pandas as pd


def get_spotify_tracks(access_token: str, n_of_tracks: int, genres: list) -> pd.DataFrame:
    """
    Retrieves a specified number of tracks for each genre from the Spotify API and returns them as a DataFrame.

    Parameters:
    access_token (str): The access token to be used for authorization in the request header.
    n_of_tracks (int): The number of tracks to retrieve for each genre.
    genres (list): A list of genres for which to retrieve tracks.

    Returns:
    pd.DataFrame: A DataFrame containing the retrieved tracks with the following columns:
        - track_id: The ID of the track.
        - track_name: The name of the track.
        - genre: The genre of the track.
        - artist_name: The name of the artist.
        - popularity: The popularity score of the track.
        - duration_ms: The duration of the track in milliseconds.
        - album_name: The name of the album.
        - total_tracks_in_album: The total number of tracks in the album.

    Raises:
    requests.exceptions.RequestException: If the request to the Spotify API fails.
    json.JSONDecodeError: If the response is not valid JSON.
    ValueError: If the response does not contain the expected data.

    Example:
    >>> get_spotify_tracks("Bearer BQ...", 10, ["rock", "pop"])
    DataFrame with columns: track_id, track_name, genre, artist_name, popularity, duration_ms, album_name, total_tracks_in_album
    """
    spotify_tracks = []

    for genre in genres:
        url = f"https://api.spotify.com/v1/search?q=genre:{genre}&type=track&limit={n_of_tracks}&sort=popularity.desc"
        items = get_data(url=url, access_token=access_token)

        for track in items["tracks"]["items"]:
            track_info = {
                "track_id": track["id"],
                "track_name": track["name"],
                "genre": genre,
                "artist_name": track["artists"][0]["name"],
                "popularity": track["popularity"],
                "duration_ms": track["duration_ms"],
                "album_name": track["album"]["name"],
                "total_tracks_in_album": track["album"]["total_tracks"],
            }
            spotify_tracks.append(track_info)

    spotify_tracks_df = pd.DataFrame(spotify_tracks)
    return spotify_tracks_df

def format_duration(duration_ms: int) -> str:
    minutes = duration_ms // 60000
    seconds = (duration_ms % 60000) // 1000
    return f"{int(minutes)} min {int(seconds)} sec"


def job() -> None:
    grant_type = "client_credentials"
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    access_token = get_access_token(client_id=client_id, client_secret=client_secret, grant_type=grant_type)

    genres = ["techno", "pop", "rock", "jazz", "classical"]

    spotify_tracks_df = get_spotify_tracks(access_token=access_token, n_of_tracks=50, genres=genres)

    # Group by genre and calculate avg values
    df_after_transformation = spotify_tracks_df.groupby("genre").agg({
        "popularity": "mean",
        "duration_ms": "mean",
        "total_tracks_in_album": "mean",
    }).reset_index()

    # Rename columns for clarity
    df_after_transformation.columns = ["genre", "avg_popularity", "avg_duration_ms", "avg_total_tracks_in_album"]
    # Calculate duration in format min and sec
    df_after_transformation["avg_duration_ms"] = df_after_transformation["avg_duration_ms"].apply(
        lambda duration_ms: format_duration(duration_ms))

    df = df_after_transformation

    # Define the list of valid genres
    valid_genres = ["techno", "pop", "rock", "jazz", "classical"]

    ge_df = great_expectations.from_pandas(df_after_transformation)

    genre_expectation = ge_df.expect_column_values_to_be_in_set("genre", valid_genres)
    print(genre_expectation)

    # Check if avg_popularity is between 0 and 100
    popularity_expectation = ge_df.expect_column_values_to_be_between("avg_popularity", 0, 100)
    print(popularity_expectation)

    # Zapisz DataFrame do pliku CSV
    df.to_csv("spotify_summary.csv", index=False)

# Define the DAG
with DAG(
    "infoShare_Academy_webinar_d2",  # DAG ID
    default_args={"owner": "airflow"},
    description="A simple DAG for webinar",
    schedule_interval=None,  # This will make it run only manually
    start_date=datetime(2024, 11, 26),  # Set to today's date
    catchup=False,
) as dag:

    # Define the task
    webinar_task = PythonOperator(
        task_id="webinar",
        python_callable=job
    )

    # Set the task sequence (only one task in this case)
    webinar_task

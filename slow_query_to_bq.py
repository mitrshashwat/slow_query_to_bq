#!/usr/bin/env python

import csv
import datetime
import json
import os
import subprocess

import pandas as pd
from google.auth import default
from google.cloud import bigquery
from google.cloud import storage

# Define global variables
client = None
bucket_name = 'your-bucket-name'
table_name = 'your-table-name'
credentials, project_id = default()

def rename_columns(df):
    """
    Rename the columns of a DataFrame to conform to snake_case naming convention.

    Args:
        df (pandas.DataFrame): The DataFrame to rename columns for.

    Returns:
        pandas.DataFrame: The DataFrame with renamed columns.
    """
    df = df.rename(columns={
        'Query_time': 'query_time',
        'Lock_time': 'lock_time',
        'Rows_sent': 'rows_sent',
        'Rows_examined': 'rows_examined',
        'Timestamp': 'timestamp',
        'User_host': 'user_host',
        'Query': 'query'
    })
    return df

def generate_gcs_url():
    """
    Generate the Google Cloud Storage URL for the slow query logs.

    Returns:
        str: The Google Cloud Storage URL for the slow query logs.
    """
    now = datetime.datetime.now() - datetime.timedelta(hours=1)
    date_str = now.strftime('%Y/%m/%d')
    hour_str = now.strftime('%H:00:00_%H:59:59')
    gs_slow_qry_json_url = f'gs://{bucket_name}/cloudsql.googleapis.com/mysql-slow.log/{date_str}/{hour_str}_S0.json'
    return gs_slow_qry_json_url

def download_json_file(gs_url):
    """
    Download the JSON file from Google Cloud Storage.

    Args:
        gs_url (str): The Google Cloud Storage URL of the JSON file to download.

    Returns:
        str: The local file path of the downloaded JSON file.
    """
    local_file_path = '/tmp/slow_query.json'
    subprocess.call(['gsutil', 'cp', gs_url, local_file_path])
    return local_file_path

def process_slow_query_logs(json_file_path):
    """
    Process the slow query logs from the downloaded JSON file.

    Args:
        json_file_path (str): The local file path of the downloaded JSON file.

    Returns:
        pandas.DataFrame: The DataFrame of processed slow query logs.
    """
    with open(json_file_path, 'r') as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    df = df[['textPayload']]
    df = df.dropna()
    df = df[df['textPayload'].str.contains('Query_time')]
    df = df['textPayload'].str.extract(r'Query_time:(\d+\.\d+)\s+Lock_time:(\d+\.\d+)\s+Rows_sent:(\d+)\s+Rows_examined:(\d+)\s+Timestamp:(\d+)\s+User_host:(.*)\s+Query:(.*)')
    df.columns = ['Query_time', 'Lock_time', 'Rows_sent', 'Rows_examined', 'Timestamp', 'User_host', 'Query']
    df = rename_columns(df)
    return df

def insert_into_bigquery(df):
    """
    Insert the processed slow query logs into a BigQuery table.

    Args:
        df (pandas.DataFrame): The DataFrame of processed slow query logs.
    """
    client = bigquery.Client(credentials=credentials, project=project_id)
    table_ref = client.dataset('your-dataset-name').table(table_name)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1

    csv_file_path = '/tmp/slow_query.csv'
    df.to_csv(csv_file_path, index=False)

    with open(csv_file_path, 'rb') as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)

    job.result()
    os.remove(csv_file_path)

def main():
    gs_url = generate_gcs_url()
    json_file_path = download_json_file(gs_url)
    df = process_slow_query_logs(json_file_path)
    insert_into_bigquery(df)

if __name__ == '__main__':
    main()

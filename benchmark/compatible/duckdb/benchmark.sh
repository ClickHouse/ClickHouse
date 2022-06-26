#!/bin/bash

# Install

sudo apt-get update
sudo apt-get install -y python3-pip
pip install duckdb psutil

# Load the data

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
gzip -d hits.csv.gz

./load.py
# 4216.5390389899985 seconds

# Run the queries

./run.sh | tee log.txt

wc -c my-db.duckdb

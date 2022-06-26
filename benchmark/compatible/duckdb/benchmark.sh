#!/bin/bash

# Install

sudo apt-get update
sudo apt-get install python3-pip
pip install duckdb psutil

# Load the data

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
gzip -d hits.csv.gz

# Run the queries

./run.expect

wc -c my-db.duckdb

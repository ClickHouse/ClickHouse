#!/bin/bash

# Install

curl https://clickhouse.com/ | sh
env DEBIAN_FRONTEND=noninteractive
sudo ./clickhouse install
sudo clickhouse start

# Load the data

clickhouse-client < create.sql

wget 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gz -d hits.tsv.gz

clickhouse-client --time --query "INSERT INTO hits FORMAT TSV" < hits.tsv

# Run the queries

./run.sh

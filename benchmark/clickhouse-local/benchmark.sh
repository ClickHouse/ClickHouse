#!/bin/bash

# Install

curl https://clickhouse.com/ | sh

# wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.parquet'
seq 0 99 | xargs -P100 -I{} bash -c 'wget --continue https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet'

# Run the queries

./run.sh

du -b hits.parquet

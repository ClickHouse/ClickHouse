#!/bin/bash

TRIES=3

cat queries.sql | while read query; do
    echo "$query";
    for i in $(seq 1 $TRIES); do
        psql -h "${HOST}" -U awsuser -d dev -p 5439 -t -c 'SET enable_result_cache_for_session = off' -c '\timing' -c "$query" | grep 'Time'
    done;
done;

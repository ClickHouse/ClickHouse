#!/bin/bash

TRIES=3

cat queries.sql | while read query; do
    echo "$query";
    for i in $(seq 1 $TRIES); do
        psql -U postgres -h "${HOST}" test -t -c '\timing' -c "$query" | grep 'Time'
    done;
done;

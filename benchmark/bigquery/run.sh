#!/bin/bash

TRIES=3

cat queries.sql | while read query; do
    echo "$query";
    for i in $(seq 1 $TRIES); do
        time bq query --use_legacy_sql=false --use_cache=false <<< "$query"
    done
done

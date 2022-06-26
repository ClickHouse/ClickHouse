#!/bin/bash

TRIES=3

cat queries.sql | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches

    echo "$query";
    for i in $(seq 1 $TRIES); do
        curl -s -G --data-urlencode "query=${query}" 'http://localhost:9000/exec?timings=true'
        echo
    done;
done;

#!/bin/bash

TRIES=3

cat queries.sql | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches

    echo "$query";
    for i in $(seq 1 $TRIES); do
        /opt/heavyai/bin/heavysql -t -p HyperInteractive <<< "${query}"
    done;
done;

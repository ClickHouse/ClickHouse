#!/bin/bash

TRIES=3

cat queries.sql | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches

    for i in $(seq 1 $TRIES); do
        mysql --password="${PASSWORD}" --host 127.0.0.1 -vvv test -e "${query}"
    done;
done;

#!/bin/bash

TRIES=3

cat queries.sql | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches

    for i in $(seq 1 $TRIES); do
        sudo docker exec memsql-ciab memsql -vvv -p"${ROOT_PASSWORD}" --database=test -e "USE test; ${query}"
    done;
done;

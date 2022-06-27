#!/bin/bash

TRIES=3

cat queries.sql | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches

    for i in $(seq 1 $TRIES); do
        sudo docker run --rm --network host mysql:5 mysql --host 127.0.0.1 --port 5029 --user=root --password=mypass --database=test -vvv -e "${query}"
    done;
done;

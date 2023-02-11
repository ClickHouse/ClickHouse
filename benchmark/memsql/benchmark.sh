#!/usr/bin/env bash

QUERIES_FILE="queries.sql"
TABLE=$1
TRIES=3

cat "$QUERIES_FILE" | sed "s/{table}/${TABLE}/g" | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    echo -n "["
    for i in $(seq 1 $TRIES); do

        RES=$(mysql -u root -h 127.0.0.1 -P 3306 --database=test -t -vvv -e "$query" 2>&1 | grep ' set ' | grep -oP '\d+\.\d+')

        [[ "$?" == "0" ]] && echo -n "$RES" || echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
done

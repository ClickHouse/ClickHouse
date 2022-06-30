#!/bin/bash

TRIES=3
cat queries.sql | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    echo -n "["
    for i in $(seq 1 $TRIES); do
        echo "{\"query\":\"$query\"}"| sed -e 's EventTime __time g' | tr -d ';' > query.json
        RES=$(curl -o /dev/null -s -w '%{time_total}\n' -XPOST -H'Content-Type: application/json' http://localhost:8888/druid/v2/sql/ -d @query.json)
        [[ "$?" == "0" ]] && echo -n "${RES}" || echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
done

#!/bin/bash

TRIES=3
cat queries.sql | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
    echo -n "["
    for i in $(seq 1 $TRIES); do
        echo "{\"sql\":\"$query  option(timeoutMs=300000)\"}"| tr -d ';' > query.json
        RES=$(curl -s -XPOST -H'Content-Type: application/json' http://localhost:8000/query/sql/ -d @query.json | jq 'if .exceptions == [] then .timeUsedMs/1000 else "-" end' )
        [[ "$?" == "0" ]] && echo -n "${RES}" || echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
done

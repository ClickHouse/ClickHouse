#!/bin/bash

TRIES=3
QUERY_NUM=1
cat queries.sql | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    echo -n "["
    for i in $(seq 1 $TRIES); do
        RES=$(curl -w 'Time: %{time_total}\n' http://default@localhost:8124/ -d "${query}" 2>&1 | grep -P '^Time: ' | sed 's/Time: //')
        [[ "$?" == "0" ]] && echo -n "${RES}" || echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "

        echo "${QUERY_NUM},${i},${RES}" >> result.csv
    done
    echo "],"

    QUERY_NUM=$((QUERY_NUM + 1))
done

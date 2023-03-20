#!/usr/bin/env bash

QUERIES_FILE="queries.sql"
TABLE=$1
TRIES=3

cat "$QUERIES_FILE" | sed "s|{table}|\"${TABLE}\"|g" | while read query; do

    echo -n "["
    for i in $(seq 1 $TRIES); do
        while true; do
            RES=$(command time -f %e -o time ./yql --clickhouse --syntax-version 1 -f empty <<< "USE chyt.hume; PRAGMA max_memory_usage = 100000000000; PRAGMA max_memory_usage_for_all_queries = 100000000000; $query" >/dev/null 2>&1 && cat time) && break;
        done

        [[ "$?" == "0" ]] && echo -n "${RES}" || echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
done

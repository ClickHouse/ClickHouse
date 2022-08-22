#!/usr/bin/env bash

QUERIES_FILE="queries.sql"
TABLE=$1
TRIES=3

if [ -x ./clickhouse ]
then
    CLICKHOUSE_CLIENT="./clickhouse client"
elif command -v clickhouse-client >/dev/null 2>&1
then
    CLICKHOUSE_CLIENT="clickhouse-client"
else
    echo "clickhouse-client is not found"
    exit 1
fi

cat "$QUERIES_FILE" | sed "s/{table}/${TABLE}/g" | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    echo -n "["
    for i in $(seq 1 $TRIES); do
        RES=$(${CLICKHOUSE_CLIENT} --time --format=Null --max_memory_usage=100G --query="$query" 2>&1)
        [[ "$?" == "0" ]] && echo -n "${RES}" || echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
done

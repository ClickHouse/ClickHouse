#!/usr/bin/env bash

QUERIES_FILE="queries.sql"
TABLE=$1
TRIES=3

PARAMS="--host ... --secure --password ..."

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

QUERY_ID_PREFIX="benchmark_$RANDOM"
QUERY_NUM=1

cat "$QUERIES_FILE" | sed "s/{table}/${TABLE}/g" | while read query
    do
    for i in $(seq 1 $TRIES)
    do
        QUERY_ID="${QUERY_ID_PREFIX}_${QUERY_NUM}_${i}"
        ${CLICKHOUSE_CLIENT} ${PARAMS} --query_id "${QUERY_ID}" --format=Null --max_memory_usage=100G --query="$query"
        echo -n '.'
    done
    QUERY_NUM=$((QUERY_NUM + 1))
    echo
done

sleep 10

${CLICKHOUSE_CLIENT} ${PARAMS} --query "
    WITH extractGroups(query_id, '(\d+)_(\d+)\$') AS num_run, num_run[1]::UInt8 AS num, num_run[2]::UInt8 AS run
    SELECT groupArrayInsertAt(query_duration_ms / 1000, (run - 1)::UInt8)::String || ','
    FROM clusterAllReplicas(default, system.query_log)
    WHERE event_date >= yesterday() AND type = 2 AND query_id LIKE '${QUERY_ID_PREFIX}%'
    GROUP BY num ORDER BY num FORMAT TSV
"

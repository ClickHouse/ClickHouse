#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_TABLE="test_insert_on_connection_drop"

N=50
INTERVAL_SEC=0.2
SETTINGS="max_insert_block_size=100,input_format_parallel_parsing=0,min_insert_block_size_bytes=1,async_insert=0"
CLICKHOUSE_INSERT_URL="${CLICKHOUSE_URL}&max_query_size=1000&idle_connection_timeout=5&receive_timeout=5&send_timeout=5&http_receive_timeout=5&http_send_timeout=5&query=INSERT%20INTO%20${CLICKHOUSE_TABLE}%20SETTINGS%20${SETTINGS//,/%2C}%20FORMAT%20JSONEachRow"


echo "DROP TABLE IF EXISTS ${CLICKHOUSE_TABLE}" | \
    curl -sS -d@- "$CLICKHOUSE_URL"


echo "CREATE TABLE ${CLICKHOUSE_TABLE} (id UInt64, data String) ENGINE = MergeTree ORDER BY id" | \
    curl -sS -d@- "$CLICKHOUSE_URL"

(
i=1
while [[ $i -le $N ]]; do
    echo "{\"id\":$i,\"data\":\"hello-$i\"}"
    ((i++))
    sleep $INTERVAL_SEC
done

sleep 31
) | curl -sS --no-buffer \
    -T - \
    -X POST \
    -H "Content-Type: application/json" \
    -H "Transfer-Encoding: chunked" \
    "$CLICKHOUSE_INSERT_URL" 2>&1


echo "SELECT count() FROM ${CLICKHOUSE_TABLE}" | \
    curl -sS -d@- "$CLICKHOUSE_URL"


echo "DROP TABLE IF EXISTS ${CLICKHOUSE_TABLE}" | \
    curl -sS -d@- "$CLICKHOUSE_URL"




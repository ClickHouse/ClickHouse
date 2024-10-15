#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

message="INSERT query will be executed synchronously because it has too much data"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_async_insert_fallback"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_async_insert_fallback (a UInt64) ENGINE = Memory"

query_id_suffix="${CLICKHOUSE_DATABASE}_${RANDOM}"

# inlined data via native protocol
$CLICKHOUSE_CLIENT \
    --query_id "0_$query_id_suffix" \
    --async_insert 1 \
    --async_insert_max_data_size 5 \
    --query "INSERT INTO t_async_insert_fallback VALUES (1) (2) (3)"

# inlined data via http
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=1_$query_id_suffix&async_insert=1&async_insert_max_data_size=3" \
    -d "INSERT INTO t_async_insert_fallback VALUES (4) (5) (6)"

# partially inlined partially sent via post data
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&query_id=2_$query_id_suffix&async_insert=1&async_insert_max_data_size=5&query=INSERT+INTO+t_async_insert_fallback+VALUES+(7)" \
    --data-binary @- <<< "(8) (9)"

# partially inlined partially sent via post data
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&query_id=3_$query_id_suffix&async_insert=1&async_insert_max_data_size=5&query=INSERT+INTO+t_async_insert_fallback+VALUES+(10)+(11)" \
    --data-binary @- <<< "(12)"

# sent via post data
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&query_id=4_$query_id_suffix&async_insert=1&async_insert_max_data_size=5&query=INSERT+INTO+t_async_insert_fallback+FORMAT+Values" \
    --data-binary @- <<< "(13) (14) (15)"

# no limit for async insert size
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&query_id=5_$query_id_suffix&async_insert=1&query=INSERT+INTO+t_async_insert_fallback+FORMAT+Values" \
    --data-binary @- <<< "(16) (17) (18)"

$CLICKHOUSE_CLIENT --query "SELECT * FROM t_async_insert_fallback ORDER BY a"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT --query "
    SELECT 'id_' || splitByChar('_', query_id)[1] AS id FROM system.text_log
    WHERE query_id LIKE '%$query_id_suffix' AND message LIKE '%$message%'
    ORDER BY id
    SETTINGS max_rows_to_read = 0
"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_async_insert_fallback"

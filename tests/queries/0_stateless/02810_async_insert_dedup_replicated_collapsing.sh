#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Slow async_insert_busy_timeout_ms

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS 02810_async_insert_dedup_collapsing"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE 02810_async_insert_dedup_collapsing (stringvalue String, sign Int8) ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/{database}/02810_async_insert_dedup', 'r1', sign) ORDER BY stringvalue"

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&async_insert_busy_timeout_ms=3000&async_insert_use_adaptive_busy_timeout=0&async_insert_deduplicate=1"

# insert value with same key and sign so it's collapsed on insert
${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO 02810_async_insert_dedup_collapsing VALUES ('string1', 1)" &
${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO 02810_async_insert_dedup_collapsing VALUES ('string1', 1)" &

wait

${CLICKHOUSE_CLIENT} -q "SELECT stringvalue FROM 02810_async_insert_dedup_collapsing ORDER BY stringvalue"
${CLICKHOUSE_CLIENT} -q "SELECT '------------'"

# trigger same collaps algorithm but also deduplication
${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO 02810_async_insert_dedup_collapsing VALUES ('string1', 1)" &
${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO 02810_async_insert_dedup_collapsing VALUES ('string1', 1)" &

wait

${CLICKHOUSE_CLIENT} -q "SELECT stringvalue FROM 02810_async_insert_dedup_collapsing ORDER BY stringvalue"
${CLICKHOUSE_CLIENT} -q "SELECT '------------'"

${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO 02810_async_insert_dedup_collapsing VALUES ('string2', 1)" &
${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO 02810_async_insert_dedup_collapsing VALUES ('string2', 1), ('string1', 1)" &
${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO 02810_async_insert_dedup_collapsing VALUES ('string2', 1)" &

wait

${CLICKHOUSE_CLIENT} -q "SELECT stringvalue FROM 02810_async_insert_dedup_collapsing ORDER BY stringvalue"
${CLICKHOUSE_CLIENT} -q "SELECT '------------'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE 02810_async_insert_dedup_collapsing"

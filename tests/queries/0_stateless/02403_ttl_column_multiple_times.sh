#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./merges.lib
. "$CURDIR"/merges.lib
set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ttl_table;"

$CLICKHOUSE_CLIENT --query "CREATE TABLE ttl_table
(
    EventDate Date,
    Longitude Float64 TTL EventDate + toIntervalWeek(2)
)
ENGINE = MergeTree()
ORDER BY EventDate
SETTINGS vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1;"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES ttl_table;"

$CLICKHOUSE_CLIENT --query "INSERT INTO ttl_table VALUES(toDate('2020-10-01'), 144);"

$CLICKHOUSE_CLIENT --query "SELECT * FROM ttl_table;"

$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ttl_table;"

$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE ttl_table FINAL;"

wait_for_merges_done ttl_table

$CLICKHOUSE_CLIENT --query "SELECT * FROM ttl_table;"

$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE ttl_table FINAL;"

$CLICKHOUSE_CLIENT --query "SELECT * FROM ttl_table;"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ttl_table;"

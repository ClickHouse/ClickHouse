#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./merges.lib
. "$CURDIR"/merges.lib
set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS table_with_column_ttl;"

$CLICKHOUSE_CLIENT --query "CREATE TABLE table_with_column_ttl
(
    EventTime DateTime,
    UserID UInt64,
    Age UInt8 TTL EventTime + INTERVAL 3 MONTH
)
ENGINE MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;" # column TTL doesn't work for compact parts

$CLICKHOUSE_CLIENT --query "INSERT INTO table_with_column_ttl VALUES (now(), 1, 32);"

$CLICKHOUSE_CLIENT --query "INSERT INTO table_with_column_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 45);"

$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE table_with_column_ttl FINAL;"

wait_for_merges_done table_with_column_ttl

$CLICKHOUSE_CLIENT --query "SELECT UserID, Age FROM table_with_column_ttl ORDER BY UserID;"

$CLICKHOUSE_CLIENT --query "ALTER TABLE table_with_column_ttl MODIFY COLUMN Age REMOVE TTL;"

$CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE table_with_column_ttl;"

$CLICKHOUSE_CLIENT --query "INSERT INTO table_with_column_ttl VALUES (now() - INTERVAL 10 MONTH, 3, 27);"

$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE table_with_column_ttl FINAL;"

$CLICKHOUSE_CLIENT --query "SELECT UserID, Age FROM table_with_column_ttl ORDER BY UserID;"

$CLICKHOUSE_CLIENT --query "DROP TABLE table_with_column_ttl;"

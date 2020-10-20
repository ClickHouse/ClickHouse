#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS minmax_idx;"


$CLICKHOUSE_CLIENT -n --query="
CREATE TABLE minmax_idx
(
    u64 UInt64,
    i64 Int64,
    i32 Int32,
    INDEX idx (i64, u64 * i64) TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY i32
ORDER BY u64
SETTINGS index_granularity = 2;"


$CLICKHOUSE_CLIENT --query="INSERT INTO minmax_idx VALUES
(0, 2, 1),
(1, 1, 1),
(2, 1, 1),
(3, 1, 1),
(4, 1, 1),
(5, 2, 1),
(6, 1, 2),
(7, 1, 2),
(8, 1, 2),
(9, 1, 2)"

$CLICKHOUSE_CLIENT --query="SELECT count() FROM minmax_idx WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM minmax_idx WHERE i64 = 2 FORMAT JSON" | grep "rows_read" # Returns 4

$CLICKHOUSE_CLIENT --query="ALTER TABLE minmax_idx CLEAR INDEX idx IN PARTITION 1;" --mutations_sync=1

$CLICKHOUSE_CLIENT --query="SELECT count() FROM minmax_idx WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM minmax_idx WHERE i64 = 2 FORMAT JSON" | grep "rows_read" # Returns 6

$CLICKHOUSE_CLIENT --query="ALTER TABLE minmax_idx MATERIALIZE INDEX idx IN PARTITION 1;" --mutations_sync=1

$CLICKHOUSE_CLIENT --query="SELECT count() FROM minmax_idx WHERE i64 = 2;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM minmax_idx WHERE i64 = 2 FORMAT JSON" | grep "rows_read" # Returns 4

$CLICKHOUSE_CLIENT --query="DROP TABLE minmax_idx"

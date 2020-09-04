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
ORDER BY u64
SETTINGS index_granularity = 2;"

$CLICKHOUSE_CLIENT --query="INSERT INTO minmax_idx VALUES
(0, 1, 1),
(1, 1, 2),
(2, 1, 3),
(3, 1, 4),
(4, 1, 5),
(5, 1, 6),
(6, 1, 7),
(7, 1, 8),
(8, 1, 9),
(9, 1, 10)"

$CLICKHOUSE_CLIENT --query="SELECT count() FROM minmax_idx WHERE i64 = 1;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM minmax_idx WHERE i64 = 5;"

$CLICKHOUSE_CLIENT --query="ALTER TABLE minmax_idx UPDATE i64 = 5 WHERE i64 = 1;" --mutations_sync=1

$CLICKHOUSE_CLIENT --query="SELECT count() FROM minmax_idx WHERE i64 = 1;"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM minmax_idx WHERE i64 = 5;"

$CLICKHOUSE_CLIENT --query="DROP TABLE minmax_idx"

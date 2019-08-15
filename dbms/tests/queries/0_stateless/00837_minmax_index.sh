#!/usr/bin/env bash

CLICKHOUSE_CLIENT_OPT="--allow_experimental_data_skipping_indices=1"

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS minmax_idx;"


$CLICKHOUSE_CLIENT -n --query="
SET allow_experimental_data_skipping_indices = 1;
CREATE TABLE minmax_idx
(
    u64 UInt64,
    i32 Int32,
    f64 Float64,
    d Decimal(10, 2),
    s String,
    e Enum8('a' = 1, 'b' = 2, 'c' = 3),
    dt Date,
    INDEX idx_all (i32, i32 + f64, d, s, e, dt) TYPE minmax GRANULARITY 1,
    INDEX idx_all2 (i32, i32 + f64, d, s, e, dt) TYPE minmax GRANULARITY 2,
    INDEX idx_2 (u64 + toYear(dt), substring(s, 2, 4)) TYPE minmax GRANULARITY 3
) ENGINE = MergeTree()
ORDER BY u64
SETTINGS index_granularity = 2;"


$CLICKHOUSE_CLIENT --query="INSERT INTO minmax_idx VALUES
(0, 5, 4.7, 6.5, 'cba', 'b', '2014-01-04'),
(1, 5, 4.7, 6.5, 'cba', 'b', '2014-03-11'),
(2, 2, 4.5, 2.5, 'abc', 'a', '2014-01-01'),
(3, 5, 6.9, 1.57, 'bac', 'c', '2017-01-01'),
(4, 2, 4.5, 2.5, 'abc', 'a', '2016-01-01'),
(5, 5, 6.9, 1.57, 'bac', 'c', '2014-11-11'),
(6, 2, 4.5, 2.5, 'abc', 'a', '2014-02-11'),
(7, 5, 6.9, 1.57, 'bac', 'c', '2014-04-11'),
(8, 2, 4.5, 2.5, 'abc', 'a', '2014-05-11'),
(9, 5, 6.9, 1.57, 'bac', 'c', '2014-07-11'),
(11, 5, 4.7, 6.5, 'cba', 'b', '2014-06-11'),
(12, 5, 4.7, 6.5, 'cba', 'b', '2015-01-01')"

# simple select
$CLICKHOUSE_CLIENT --query="SELECT * FROM minmax_idx WHERE i32 = 5 AND i32 + f64 < 12 AND 3 < d AND d < 7 AND (s = 'bac' OR s = 'cba') ORDER BY dt"
$CLICKHOUSE_CLIENT --query="SELECT * FROM minmax_idx WHERE i32 = 5 AND i32 + f64 < 12 AND 3 < d AND d < 7 AND (s = 'bac' OR s = 'cba') ORDER BY dt FORMAT JSON" | grep "rows_read"

# select with hole made by primary key
$CLICKHOUSE_CLIENT --query="SELECT * FROM minmax_idx WHERE (u64 < 2 OR u64 > 10) AND e != 'b' ORDER BY dt"
$CLICKHOUSE_CLIENT --query="SELECT * FROM minmax_idx WHERE (u64 < 2 OR u64 > 10) AND e != 'b' ORDER BY dt FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="DROP TABLE minmax_idx"
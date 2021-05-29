#!/usr/bin/env bash


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS set_idx;"

$CLICKHOUSE_CLIENT -n --query="
CREATE TABLE set_idx
(
    u64 UInt64,
    i32 Int32,
    f64 Float64,
    d Decimal(10, 2),
    s String,
    e Enum8('a' = 1, 'b' = 2, 'c' = 3),
    dt Date,
    INDEX idx_all (i32, i32 + f64, d, s, e, dt) TYPE set(2) GRANULARITY 1,
    INDEX idx_all2 (i32, i32 + f64, d, s, e, dt) TYPE set(4) GRANULARITY 2,
    INDEX idx_2 (u64 + toYear(dt), substring(s, 2, 4)) TYPE set(6) GRANULARITY 3
) ENGINE = MergeTree()
ORDER BY u64
SETTINGS index_granularity = 2;"

$CLICKHOUSE_CLIENT --query="INSERT INTO set_idx VALUES
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
(12, 5, 4.7, 6.5, 'cba', 'b', '2014-06-11'),
(13, 5, 4.7, 6.5, 'cba', 'b', '2015-01-01')"

# simple select
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE i32 = 5 AND i32 + f64 < 12 AND 3 < d AND d < 7 AND (s = 'bac' OR s = 'cba') ORDER BY dt"
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE i32 = 5 AND i32 + f64 < 12 AND 3 < d AND d < 7 AND (s = 'bac' OR s = 'cba') ORDER BY dt FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE NOT (i32 = 5 AND i32 + f64 < 12 AND 3 < d AND d < 7 AND (s = 'bac' OR s = 'cba'))  ORDER BY dt"
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE NOT (i32 = 5 AND i32 + f64 < 12 AND 3 < d AND d < 7 AND (s = 'bac' OR s = 'cba')) ORDER BY dt FORMAT JSON" | grep "rows_read"

# select with hole made by primary key
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE (u64 < 2 OR u64 > 10) AND s != 'cba' ORDER BY dt"
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE (u64 < 2 OR u64 > 10) AND s != 'cba' ORDER BY dt FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE (u64 < 2 OR NOT u64 > 10) AND NOT (s = 'cba') ORDER BY dt"
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE (u64 < 2 OR NOT u64 > 10) AND NOT (s = 'cba') ORDER BY dt FORMAT JSON" | grep "rows_read"


$CLICKHOUSE_CLIENT --query="DROP TABLE set_idx;"

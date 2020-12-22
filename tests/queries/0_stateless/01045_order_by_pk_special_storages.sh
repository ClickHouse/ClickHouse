#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS s1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS s2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS m"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS buf"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv"

$CLICKHOUSE_CLIENT -q "CREATE TABLE s1 (a UInt32, s String) ENGINE = MergeTree ORDER BY a PARTITION BY a % 3 SETTINGS min_bytes_for_wide_part = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE s2 (a UInt32, s String) ENGINE = MergeTree ORDER BY a PARTITION BY a % 3 SETTINGS min_bytes_for_wide_part = 0"

$CLICKHOUSE_CLIENT -q "CREATE TABLE m (a UInt32, s String) engine = Merge('$CLICKHOUSE_DATABASE', 's[1,2]')"
$CLICKHOUSE_CLIENT -q "INSERT INTO s1 select (number % 20) * 2 as n, toString(number * number) from numbers(100000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO s2 select (number % 20) * 2 + 1 as n, toString(number * number * number) from numbers(100000)"

$CLICKHOUSE_CLIENT -q "SELECT '---StorageMerge---'"
$CLICKHOUSE_CLIENT -q "SELECT a FROM m ORDER BY a LIMIT 5"
$CLICKHOUSE_CLIENT -q "SELECT a, s FROM m ORDER BY a, s LIMIT 10"

# Not a single .sql test with max_rows_to_read because it doesn't work with Merge storage
rows_read=$($CLICKHOUSE_CLIENT -q "SELECT a FROM m ORDER BY a LIMIT 10 FORMAT JSON" --max_threads=1 --max_block_size=20 | grep "rows_read" | sed 's/[^0-9]*//g')

# Expected number of read rows with a bit margin
if [[ $rows_read -lt 500 ]]
    then echo "OK"
else
    echo "FAIL"
fi

$CLICKHOUSE_CLIENT -q "SELECT '---StorageBuffer---'"
$CLICKHOUSE_CLIENT -q "CREATE TABLE buf (a UInt32, s String) engine = Buffer('$CLICKHOUSE_DATABASE', s2, 16, 10, 100, 10000, 1000000, 10000000, 100000000)"
$CLICKHOUSE_CLIENT -q "SELECT a, s FROM buf ORDER BY a, s LIMIT 10"
rows_read=$($CLICKHOUSE_CLIENT -q "SELECT a FROM buf ORDER BY a LIMIT 10 FORMAT JSON" --max_threads=1 --max_block_size=20 | grep "rows_read" | sed 's/[^0-9]*//g')

# Expected number of read rows with a bit margin
if [[ $rows_read -lt 500 ]]
    then echo "OK"
else
    echo "FAIL"
fi

$CLICKHOUSE_CLIENT -q "SELECT '---MaterializedView---'"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW mv (a UInt32, s String) engine = MergeTree ORDER BY s SETTINGS min_bytes_for_wide_part = 0 POPULATE AS SELECT a, s FROM s1 WHERE a % 7 = 0"
$CLICKHOUSE_CLIENT -q "SELECT a, s FROM mv ORDER BY s LIMIT 10"
rows_read=$($CLICKHOUSE_CLIENT -q "SELECT a, s FROM mv ORDER BY s LIMIT 10 FORMAT JSON" --max_threads=1 --max_block_size=20 | grep "rows_read" | sed 's/[^0-9]*//g')

if [[ $rows_read -lt 500 ]]
    then echo "OK"
else
    echo "FAIL"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS s1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS s2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS m"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS buf"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv"

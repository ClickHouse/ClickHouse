#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function check_if_optimzed()
{
    query="$1"
    echo $query
    ! $CLICKHOUSE_CLIENT --max_threads 8 --optimize_read_in_order 1 -q "EXPLAIN PIPELINE $query" | grep -q "MergeSorting"
}

function assert_optimized()
{
    check_if_optimzed "$1" && echo "OK" || echo "FAIL"
}

function assert_not_optimized()
{
    ! check_if_optimzed "$1" && echo "OK" || echo "FAIL"
}

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_fixed_prefix"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_fixed_prefix (a UInt32, b UInt32, c UInt32, d UInt32, e UInt32)
    ENGINE = MergeTree ORDER BY (a, b, c, d)"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_fixed_prefix"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_fixed_prefix SELECT number % 2, number % 10, number % 100, number % 1000, number FROM numbers(100000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_fixed_prefix SELECT number % 2, number % 10, number % 100, number % 1000, number FROM numbers(100000)"

assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix ORDER BY a"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix ORDER BY a, b"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix ORDER BY a, b, c"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix ORDER BY a, b, c, d"

assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 ORDER BY a"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 ORDER BY a, b"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 ORDER BY b"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 ORDER BY b, c"

assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE b = 1 ORDER BY a"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE b = 1 ORDER BY a, c"
assert_not_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE b = 1 ORDER BY b, c"

assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE c = 1 ORDER BY a"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE c = 1 ORDER BY a, b"

assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND b = 1 ORDER BY a"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND b = 1 ORDER BY a, b"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND b = 1 ORDER BY a, c"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND b = 1 ORDER BY a, b, c"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND b = 1 ORDER BY a, b, c, d"

assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND b = 1 ORDER BY b, a"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND b = 1 ORDER BY b, c"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND b = 1 ORDER BY b, a, c"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND b = 1 ORDER BY c, d"

assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND c = 1 ORDER BY a"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND c = 1 ORDER BY a, b"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND c = 1 ORDER BY a, c"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND c = 1 ORDER BY b, d"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND c = 1 ORDER BY a, b, c"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND c = 1 ORDER BY b, c, d"
assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 AND c = 1 ORDER BY a, b, c, d"

assert_not_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix ORDER BY b"
assert_not_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix ORDER BY b, a"
assert_not_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix ORDER BY b, c"

assert_not_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 ORDER BY c"
assert_not_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 ORDER BY c, b"
assert_not_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 ORDER BY c, d"

assert_not_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE c = 1 ORDER BY c, d"
assert_not_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE c = 1 ORDER BY b, c"

assert_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 OR b = 1 ORDER BY a, b"
assert_not_optimized "SELECT a, b, c, d, e FROM t_fixed_prefix WHERE a = 1 OR b = 1 ORDER BY b"

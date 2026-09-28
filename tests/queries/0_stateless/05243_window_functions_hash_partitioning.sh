#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `query_plan_window_functions_hash_partitioning` must not change any result, and must be used only for
# aggregate functions over whole partitions.

# Hash partitioning is not used when the storage ordering may be reused for the window sort.
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --query_plan_reuse_storage_ordering_for_window_functions=0"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t (k Int32, s String, lc LowCardinality(String), n Nullable(Int32), d Decimal(10, 2), f Float64, x Int64, y Int64)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number % 7, toString(number % 5), toString(number % 3), if(number % 4 = 0, NULL, number % 6),
    (number % 9) / 4, (number % 8) / 2, number, (number * 7919) % 1000 FROM numbers(1000);
CREATE TABLE e (k Int32, x Int64) ENGINE = MergeTree ORDER BY tuple();
"

# The queries are collected into scripts and run at the end with few client invocations, because starting
# the client is slow in sanitizer builds.
script_off=""
script_on=""

function emit()
{
    script_off+="$1"$'\n'
    script_on+="$2"$'\n'
}

function check()
{
    emit "SELECT '=== $1 ==='; $2;" \
        "SELECT format('=== {}: hash partitioning {} ===', '$1', count()) FROM (EXPLAIN actions = 1 $2) WHERE explain LIKE '%Hash partitioning: 1%'; $2;"
}

function section()
{
    emit "SELECT '--- $1 ---';" "SELECT '--- $1 ---';"
}

# Every query returns a digest over all rows, so the row order does not matter.
function digest()
{
    echo "SELECT count(), sum(cityHash64(*)) FROM ($1)"
}

section "hash partitioning"
check "max, min, sum, count" "$(digest "SELECT x, max(x) OVER w, min(y) OVER w, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY k)")"
check "avg over Decimal" "$(digest "SELECT x, avg(d) OVER (PARTITION BY k) FROM t")"
check "parametric" "$(digest "SELECT x, quantileExact(0.5)(y) OVER (PARTITION BY k), groupArraySorted(3)(y) OVER (PARTITION BY k) FROM t")"
check "two arguments" "$(digest "SELECT x, argMax(x, y) OVER (PARTITION BY k) FROM t")"
check "combinators" "$(digest "SELECT x, sumIf(x, y > 500) OVER (PARTITION BY k), uniqExact(y) OVER (PARTITION BY k), countDistinct(s) OVER (PARTITION BY k) FROM t")"
check "-State" "$(digest "SELECT x, finalizeAggregation(sumState(x) OVER (PARTITION BY k)) FROM t")"
check "String key" "$(digest "SELECT x, max(y) OVER (PARTITION BY s) FROM t")"
check "LowCardinality key" "$(digest "SELECT x, max(y) OVER (PARTITION BY lc) FROM t")"
check "Nullable key and argument" "$(digest "SELECT x, max(n) OVER (PARTITION BY n), count(n) OVER (PARTITION BY n) FROM t")"
check "Decimal key" "$(digest "SELECT x, sum(x) OVER (PARTITION BY d) FROM t")"
check "several keys" "$(digest "SELECT x, sum(x) OVER (PARTITION BY k, s, lc) FROM t")"
check "expression key" "$(digest "SELECT x, sum(x) OVER (PARTITION BY k % 3, lower(s)) FROM t")"
check "ROWS UNBOUNDED to UNBOUNDED" "$(digest "SELECT x, sum(x) OVER (PARTITION BY k ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t")"
check "RANGE CURRENT ROW to UNBOUNDED" "$(digest "SELECT x, sum(x) OVER (PARTITION BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t")"
check "two windows" "$(digest "SELECT x, sum(x) OVER (PARTITION BY k), max(y) OVER (PARTITION BY s) FROM t")"
check "with a sorted window" "$(digest "SELECT x, sum(x) OVER (PARTITION BY k), row_number() OVER (PARTITION BY k ORDER BY x) FROM t")"
check "ORDER BY another column" "SELECT x, sum(x) OVER (PARTITION BY k) FROM t ORDER BY x LIMIT 5"
check "constant column" "$(digest "SELECT 42 AS c, x, max(x) OVER (PARTITION BY k) FROM t")"
check "empty input" "$(digest "SELECT x, max(x) OVER (PARTITION BY k) FROM e")"
check "one thread" "$(digest "SELECT x, sum(x) OVER (PARTITION BY k) FROM t SETTINGS max_threads = 1")"
check "spilling to disk" "$(digest "SELECT x, sum(x) OVER (PARTITION BY k), max(s) OVER (PARTITION BY k) FROM t SETTINGS max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0, max_block_size = 10")"
check "spilling with a constant column" "$(digest "SELECT 42 AS c, x, sum(x) OVER (PARTITION BY k) FROM t SETTINGS max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0, max_block_size = 10, max_threads = 2")"
check "filter on the partition key" "SELECT k, x, sum(x) OVER (PARTITION BY k) AS c FROM t QUALIFY k = 3 ORDER BY x LIMIT 3"
check "max_rows_to_sort" "$(digest "SELECT x, sum(x) OVER (PARTITION BY k) FROM t SETTINGS max_rows_to_sort = 1000000, max_bytes_to_sort = 1000000000")"
check "max_rows_to_sort per stream with one thread" "$(digest "SELECT x, sum(x) OVER (PARTITION BY k) FROM (SELECT * FROM t UNION ALL SELECT * FROM t) SETTINGS max_threads = 1, max_rows_to_sort = 1500")"
check "LIMIT" "SELECT k, sum(x) OVER (PARTITION BY k) AS c FROM t WHERE x < 20 ORDER BY x LIMIT 3"

section "sorting"
check "ORDER BY in the window" "$(digest "SELECT x, sum(x) OVER (PARTITION BY k ORDER BY x) FROM t")"
check "ROWS frame up to the current row" "$(digest "SELECT x, sum(x) OVER (PARTITION BY k ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), sum(x) OVER (PARTITION BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t")"
check "window function" "$(digest "SELECT x, row_number() OVER (PARTITION BY k), max(x) OVER (PARTITION BY k) FROM t")"
check "no PARTITION BY" "$(digest "SELECT x, sum(x) OVER () FROM t")"
check "Float key" "$(digest "SELECT x, sum(x) OVER (PARTITION BY f) FROM t")"
check "ORDER BY the partition key" "SELECT k, sum(x) OVER (PARTITION BY k) AS c FROM t ORDER BY k LIMIT 3"
check "ORDER BY a prefix of the partition key" "SELECT k, sum(x) OVER (PARTITION BY k, k % 2) AS c FROM t ORDER BY k LIMIT 3"
check "reuse of the storage ordering" "$(digest "SELECT x, sum(x) OVER (PARTITION BY k) FROM t SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1, optimize_read_in_order = 1, query_plan_read_in_order = 1")"

off=$($CLICKHOUSE_CLIENT --query_plan_window_functions_hash_partitioning=0 -q "$script_off")
on=$($CLICKHOUSE_CLIENT --query_plan_window_functions_hash_partitioning=1 -q "$script_on")
echo "$on"
if [ "$off" != "$(echo "$on" | sed -E 's/: hash partitioning [0-9]+ ===$/ ===/')" ]; then
    echo "DIFFERENT FROM THE RESULT WITH SORTING:"
    echo "$off"
fi

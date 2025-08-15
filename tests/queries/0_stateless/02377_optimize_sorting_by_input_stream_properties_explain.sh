#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISABLE_OPTIMIZATION="set optimize_sorting_by_input_stream_properties=0;set query_plan_read_in_order=0;set max_threads=3"
ENABLE_OPTIMIZATION="set optimize_sorting_by_input_stream_properties=1;set query_plan_read_in_order=1;set optimize_read_in_order=1;set max_threads=3"
MAKE_OUTPUT_STABLE="set optimize_read_in_order=1;set max_threads=3;set query_plan_remove_redundant_sorting=0"
GREP_SORTING="grep 'PartialSortingTransform\|LimitsCheckingTransform\|MergeSortingTransform\|MergingSortedTransform'"
GREP_SORTMODE="grep 'Sorting ('"
TRIM_LEADING_SPACES="sed -e 's/^[ \t]*//'"
FIND_SORTING="$GREP_SORTING | $TRIM_LEADING_SPACES"
FIND_SORTMODE="$GREP_SORTMODE | $TRIM_LEADING_SPACES"

function explain_sorting {
    echo "-- QUERY: "$1
    $CLICKHOUSE_CLIENT --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0.0 -nq "$1" | eval $FIND_SORTING
}

function explain_sortmode {
    echo "-- QUERY: "$1
    $CLICKHOUSE_CLIENT --enable_analyzer=0 --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0.0 -nq "$1" | eval $FIND_SORTMODE
    echo "-- QUERY (analyzer): "$1
    $CLICKHOUSE_CLIENT --enable_analyzer=1 --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0.0 -nq "$1" | eval $FIND_SORTMODE
}

$CLICKHOUSE_CLIENT -q "drop table if exists optimize_sorting sync"
$CLICKHOUSE_CLIENT -q "create table optimize_sorting (a UInt64, b UInt64, c UInt64) engine=MergeTree() order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into optimize_sorting values (0, 0, 0) (1, 1, 1)"
echo "-- EXPLAIN PLAN sorting for MergeTree w/o sorting key"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a FROM optimize_sorting ORDER BY a"
$CLICKHOUSE_CLIENT -q "drop table if exists optimize_sorting sync"

$CLICKHOUSE_CLIENT -q "drop table if exists optimize_sorting sync"
$CLICKHOUSE_CLIENT -q "create table optimize_sorting (a UInt64, b UInt64, c UInt64) engine=MergeTree() order by (a, b)"
$CLICKHOUSE_CLIENT -q "insert into optimize_sorting select number, number % 5, number % 2 from numbers(0,10)"
$CLICKHOUSE_CLIENT -q "insert into optimize_sorting select number, number % 5, number % 2 from numbers(10,10)"
$CLICKHOUSE_CLIENT -q "insert into optimize_sorting SELECT number, number % 5, number % 2 from numbers(20,10)"

echo "-- disable optimization -> sorting order is NOT propagated from subquery -> full sort"
explain_sorting "$DISABLE_OPTIMIZATION;EXPLAIN PIPELINE SELECT a FROM (SELECT a FROM optimize_sorting) ORDER BY a"

echo "-- enable optimization -> sorting order is propagated from subquery -> merge sort"
explain_sorting "$ENABLE_OPTIMIZATION;EXPLAIN PIPELINE SELECT a FROM (SELECT a FROM optimize_sorting) ORDER BY a"
echo "-- enable optimization -> there is no sorting order to propagate from subquery -> full sort"
explain_sorting "$ENABLE_OPTIMIZATION;EXPLAIN PIPELINE SELECT c FROM (SELECT c FROM optimize_sorting) ORDER BY c"

echo "-- ExpressionStep preserves sort mode"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a FROM optimize_sorting ORDER BY a"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a FROM optimize_sorting ORDER BY a+1"

echo "-- ExpressionStep breaks sort mode"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a+1 FROM optimize_sorting ORDER BY a+1"

echo "-- FilterStep preserves sort mode"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a FROM optimize_sorting WHERE a > 0"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a FROM optimize_sorting WHERE a+1 > 0"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a, a+1 FROM optimize_sorting WHERE a+1 > 0"

echo "-- FilterStep breaks sort mode"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a > 0 FROM optimize_sorting WHERE a > 0"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a+1 FROM optimize_sorting WHERE a+1 > 0"

echo "-- aliases break sorting order"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a FROM (SELECT sipHash64(a) AS a FROM (SELECT a FROM optimize_sorting ORDER BY a)) ORDER BY a"

echo "-- aliases DONT break sorting order"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a, b FROM (SELECT x AS a, y AS b FROM (SELECT a AS x, b AS y FROM optimize_sorting) ORDER BY x, y)"

echo "-- actions chain breaks sorting order: input(column a)->sipHash64(column a)->alias(sipHash64(column a), a)->plus(alias a, 1)"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a, z FROM (SELECT sipHash64(a) AS a, a + 1 AS z FROM (SELECT a FROM optimize_sorting ORDER BY a + 1)) ORDER BY a + 1"

echo "-- check that correct sorting info is provided in case of only prefix of sorting key is in ORDER BY clause but all sorting key columns returned by query"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN sorting=1 SELECT a, b FROM optimize_sorting ORDER BY a"

$CLICKHOUSE_CLIENT -q "drop table if exists optimize_sorting sync"

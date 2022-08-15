#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISABLE_OPTIMIZATION="set optimize_sorting_for_input_stream=0;set max_threads=1"
ENABLE_OPTIMIZATION="set optimize_sorting_for_input_stream=1;set max_threads=1"
MAKE_OUTPUT_STABLE="set optimize_read_in_order=1"
GREP_SORTING="grep 'PartialSortingTransform\|LimitsCheckingTransform\|MergeSortingTransform\|MergingSortedTransform'"
GREP_SORTMODE="grep 'Sorting'"
TRIM_LEADING_SPACES="sed -e 's/^[ \t]*//'"
FIND_SORTING="$GREP_SORTING | $TRIM_LEADING_SPACES"
FIND_SORTMODE="$GREP_SORTMODE | $TRIM_LEADING_SPACES"

function explain_sorting {
    echo "-- QUERY: "$1
    $CLICKHOUSE_CLIENT -nq "$1" | eval $FIND_SORTING
}
function explain_sortmode {
    echo "-- QUERY: "$1
    $CLICKHOUSE_CLIENT -nq "$1" | eval $FIND_SORTMODE
}

$CLICKHOUSE_CLIENT -q "drop table if exists optimize_sorting sync"
$CLICKHOUSE_CLIENT -q "create table optimize_sorting (a UInt64, b UInt64, c UInt64) engine=MergeTree() order by (a, b)"
$CLICKHOUSE_CLIENT -q "insert into optimize_sorting select number, number % 5, number % 2 from numbers(0,10)"
$CLICKHOUSE_CLIENT -q "insert into optimize_sorting select number, number % 5, number % 2 from numbers(10,10)"
$CLICKHOUSE_CLIENT -q "insert into optimize_sorting SELECT number, number % 5, number % 2 from numbers(20,10)"

echo "-- disable optimization -> sorting order is NOT propagated from subquery -> full sort"
explain_sorting "$DISABLE_OPTIMIZATION;EXPLAIN PIPELINE SELECT a FROM (SELECT a FROM optimize_sorting) ORDER BY a"

echo "-- enable_optimization -> sorting order is propagated from subquery -> merge sort"
explain_sorting "$ENABLE_OPTIMIZATION;EXPLAIN PIPELINE SELECT a FROM (SELECT a FROM optimize_sorting) ORDER BY a"

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

# FIXME: we still do full sort here, - it's because, for most inner subqueury, sorting description contains original column names but header contains only aliases on those columns:
#|     Header: x Int32                                                 │
#│             y Int32                                                 │
#│     Sort Mode: Chunk: a ASC, b ASC                                  │
echo "-- aliases DONT break sorting order"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a, b FROM (SELECT x AS a, y AS b FROM (SELECT a AS x, b AS y FROM optimize_sorting) ORDER BY x, y)"

echo "-- actions chain breaks sorting order: input(column a)->sipHash64(column a)->alias(sipHash64(column a), a)->plus(alias a, 1)"
explain_sortmode "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN actions=1, header=1, sorting=1 SELECT a, z FROM (SELECT sipHash64(a) AS a, a + 1 AS z FROM (SELECT a FROM optimize_sorting ORDER BY a + 1)) ORDER BY a + 1"

#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISABLE_OPTIMIZATION="set optimize_sorting_for_input_stream=0;set max_threads=1"
ENABLE_OPTIMIZATION="set optimize_sorting_for_input_stream=1;set max_threads=1"
GREP_SORTING="grep 'PartialSortingTransform\|LimitsCheckingTransform\|MergeSortingTransform\|MergingSortedTransform'"
GREP_SORTMODE="grep 'Sort Mode'"
MAKE_OUTPUT_STABLE="set optimize_read_in_order=1"
TRIM_LEADING_SPACES="sed -e 's/^[ \t]*//'"
FIND_SORTING="$GREP_SORTING | $TRIM_LEADING_SPACES"
FIND_SORTMODE="$GREP_SORTMODE | $TRIM_LEADING_SPACES"

$CLICKHOUSE_CLIENT -q "drop table if exists optimize_sorting sync"
$CLICKHOUSE_CLIENT -q "create table optimize_sorting (a UInt64, b UInt64, c UInt64) engine=MergeTree() order by (a, b)"
$CLICKHOUSE_CLIENT -q "insert into optimize_sorting select number, number % 5, number % 2 from numbers(0,10)"
$CLICKHOUSE_CLIENT -q "insert into optimize_sorting select number, number % 5, number % 2 from numbers(10,10)"
$CLICKHOUSE_CLIENT -q "insert into optimize_sorting select number, number % 5, number % 2 from numbers(20,10)"

$CLICKHOUSE_CLIENT -q "select '-- disable optimize_sorting_for_input_stream'"
$CLICKHOUSE_CLIENT -q "select '-- PIPELINE: sorting order is NOT propagated from subquery -> full sort'"
$CLICKHOUSE_CLIENT -nq "$DISABLE_OPTIMIZATION;explain pipeline select a from (select a from optimize_sorting) order by a" | eval $FIND_SORTING

$CLICKHOUSE_CLIENT -q "select '-- enable optimize_sorting_for_input_stream'"
$CLICKHOUSE_CLIENT -q "select '-- PIPELINE: sorting order is propagated from subquery -> merge sort'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select a from (select a from optimize_sorting) order by a" | eval $FIND_SORTING

$CLICKHOUSE_CLIENT -q "select '-- PLAN: ExpressionStep preserves sort mode'"
$CLICKHOUSE_CLIENT -nq "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN sortmode=1 SELECT a FROM optimize_sorting ORDER BY a" | eval $FIND_SORTMODE

$CLICKHOUSE_CLIENT -q "select '-- PLAN: ExpressionStep breaks sort mode'"
$CLICKHOUSE_CLIENT -nq "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN sortmode=1 SELECT a+1 FROM optimize_sorting ORDER BY a+1" | eval $FIND_SORTMODE

$CLICKHOUSE_CLIENT -q "select '-- PLAN: FilterStep preserves sort mode'"
$CLICKHOUSE_CLIENT -nq "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN sortmode=1 SELECT a FROM optimize_sorting WHERE a > 0" | eval $FIND_SORTMODE

$CLICKHOUSE_CLIENT -q "select '-- PLAN: FilterStep breaks sort mode'"
$CLICKHOUSE_CLIENT -nq "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN sortmode=1 SELECT a > 0 FROM optimize_sorting WHERE a > 0" | eval $FIND_SORTMODE

$CLICKHOUSE_CLIENT -q "select '-- PLAN: aliases break sorting order'"
$CLICKHOUSE_CLIENT -nq "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN sortmode=1 SELECT a FROM (SELECT sipHash64(a) AS a FROM (SELECT a FROM optimize_sorting ORDER BY a)) ORDER BY a" | eval $FIND_SORTMODE

# FIXME: we still do full sort here, - it's because, for most inner subqueury, sorting description contains original column names but header contains only aliases on those columns:
#|     Header: x Int32                                                 │
#│             y Int32                                                 │
#│     Sort Mode: Chunk: a ASC, b ASC                                  │
$CLICKHOUSE_CLIENT -q "select '-- PLAN: aliases DONT break sorting order'"
$CLICKHOUSE_CLIENT -nq "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN sortmode=1 SELECT a, b FROM (SELECT x AS a, y AS b FROM (SELECT a AS x, b AS y FROM optimize_sorting) ORDER BY x, y)" | eval $FIND_SORTMODE

$CLICKHOUSE_CLIENT -q "select '-- PLAN: actions chain breaks sorting order: input(column a)->sipHash64(column a)->alias(sipHash64(column a), a)->plus(alias a, 1)'"
$CLICKHOUSE_CLIENT -nq "$MAKE_OUTPUT_STABLE;EXPLAIN PLAN sortmode = 1 SELECT a, z FROM (SELECT sipHash64(a) AS a, a + 1 AS z FROM (SELECT a FROM optimize_sorting ORDER BY a + 1)) ORDER BY a + 1" | eval $FIND_SORTMODE

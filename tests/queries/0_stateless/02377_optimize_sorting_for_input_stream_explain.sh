#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISABLE_OPTIMIZATION="set optimize_sorting_for_input_stream=0;set max_threads=1"
ENABLE_OPTIMIZATION="set optimize_sorting_for_input_stream=1;set max_threads=1"
GREP_SORTING="grep 'PartialSortingTransform\|LimitsCheckingTransform\|MergeSortingTransform\|MergingSortedTransform'"
GREP_SORTMODE="grep 'Sort Mode'"
TRIM_LEADING_SPACES="sed -e 's/^[ \t]*//'"
FIND_SORTING="$GREP_SORTING | $TRIM_LEADING_SPACES"
FIND_SORTMODE="$GREP_SORTMODE | $TRIM_LEADING_SPACES"

$CLICKHOUSE_CLIENT -q "drop table if exists optimize_sorting sync"
$CLICKHOUSE_CLIENT -q "create table optimize_sorting (a int, b int, c int) engine=MergeTree() order by (a, b)"
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
$CLICKHOUSE_CLIENT -nq "EXPLAIN PLAN sortmode=1 SELECT a FROM optimize_sorting ORDER BY a" | eval $FIND_SORTMODE

$CLICKHOUSE_CLIENT -q "select '-- PLAN: ExpressionStep breaks sort mode'"
$CLICKHOUSE_CLIENT -nq "EXPLAIN PLAN sortmode=1 SELECT a+1 FROM optimize_sorting ORDER BY a+1" | eval $FIND_SORTMODE

$CLICKHOUSE_CLIENT -q "select '-- PLAN: FilterStep preserves sort mode'"
$CLICKHOUSE_CLIENT -nq "EXPLAIN PLAN sortmode=1 SELECT a FROM optimize_sorting WHERE a > 0" | eval $FIND_SORTMODE

$CLICKHOUSE_CLIENT -q "select '-- PLAN: FilterStep breaks sort mode'"
$CLICKHOUSE_CLIENT -nq "EXPLAIN PLAN sortmode=1 SELECT a > 0 FROM optimize_sorting WHERE a > 0" | eval $FIND_SORTMODE

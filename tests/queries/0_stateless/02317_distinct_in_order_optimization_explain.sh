#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

[ ! -z "$CLICKHOUSE_CLIENT_REDEFINED" ] && CLICKHOUSE_CLIENT=$CLICKHOUSE_CLIENT_REDEFINED

DISABLE_OPTIMIZATION="set optimize_distinct_in_order=0"
ENABLE_OPTIMIZATION="set optimize_distinct_in_order=1"
GREP_DISTINCT="grep 'DistinctSortedChunkTransform\|DistinctSortedStreamTransform\|DistinctSortedTransform\|DistinctTransform'"
TRIM_LEADING_SPACES="sed -e 's/^[ \t]*//'"
FIND_DISTINCT="$GREP_DISTINCT | $TRIM_LEADING_SPACES"

$CLICKHOUSE_CLIENT -q "drop table if exists distinct_in_order_explain sync"
$CLICKHOUSE_CLIENT -q "create table distinct_in_order_explain (a int, b int, c int) engine=MergeTree() order by (a, b, c)"
$CLICKHOUSE_CLIENT -q "insert into distinct_in_order_explain select number % number, number % 5, number % 10 from numbers(1,10)"

$CLICKHOUSE_CLIENT -q "select '-- disable optimize_distinct_in_order'"
$CLICKHOUSE_CLIENT -q "select '-- distinct all primary key columns -> no optimizations'"
$CLICKHOUSE_CLIENT -nq "$DISABLE_OPTIMIZATION;explain pipeline select distinct * from distinct_in_order_explain" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- enable optimize_distinct_in_order'"
$CLICKHOUSE_CLIENT -q "select '-- distinct with all primary key columns -> pre-distinct optimization only'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct * from distinct_in_order_explain" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with primary key prefix -> pre-distinct optimization only'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct a, c from distinct_in_order_explain" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with primary key prefix and order by on column in distinct -> pre-distinct and final distinct optimization'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct a, c from distinct_in_order_explain order by c" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with primary key prefix and order by on the same columns -> pre-distinct and final distinct optimization'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct a, b from distinct_in_order_explain order by a, b" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with primary key prefix and order by on column _not_ in distinct -> pre-distinct optimization only'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct a, c from distinct_in_order_explain order by b" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with non-primary key prefix -> no optimizations'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct b, c from distinct_in_order_explain" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with non-primary key prefix and order by on column in distinct -> final distinct optimization only'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct b, c from distinct_in_order_explain order by b" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with non-primary key prefix and order by on column _not_ in distinct -> no optimizations'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct b, c from distinct_in_order_explain order by a" | eval $FIND_DISTINCT

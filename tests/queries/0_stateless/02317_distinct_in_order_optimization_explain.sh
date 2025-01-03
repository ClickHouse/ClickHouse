#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

[ ! -z "$CLICKHOUSE_CLIENT_REDEFINED" ] && CLICKHOUSE_CLIENT=$CLICKHOUSE_CLIENT_REDEFINED

DISABLE_OPTIMIZATION="set optimize_distinct_in_order=0"
ENABLE_OPTIMIZATION="set optimize_distinct_in_order=1"
GREP_DISTINCT="grep 'DistinctSortedChunkTransform\|DistinctSortedStreamTransform\|DistinctSortedTransform\|DistinctTransform'"
TRIM_LEADING_SPACES="sed -e 's/^[ \t]*//'"
REMOVE_NON_LETTERS="sed 's/[^a-zA-Z]//g'"
FIND_DISTINCT="$GREP_DISTINCT | $TRIM_LEADING_SPACES | $REMOVE_NON_LETTERS"
FIND_READING_IN_ORDER="grep -o 'algorithm: InOrder' | $TRIM_LEADING_SPACES"
FIND_READING_DEFAULT="grep -o 'algorithm: Thread' | $TRIM_LEADING_SPACES"
FIND_SORTING_PROPERTIES="grep 'Sorting (Stream)' | $TRIM_LEADING_SPACES"

$CLICKHOUSE_CLIENT -q "drop table if exists distinct_in_order_explain sync"
$CLICKHOUSE_CLIENT -q "create table distinct_in_order_explain (a int, b int, c int) engine=MergeTree() order by (a, b)"
$CLICKHOUSE_CLIENT -q "insert into distinct_in_order_explain select number % number, number % 5, number % 10 from numbers(1,10)"
$CLICKHOUSE_CLIENT -q "insert into distinct_in_order_explain select number % number, number % 5, number % 10 from numbers(1,10)"

$CLICKHOUSE_CLIENT -q "select '-- disable optimize_distinct_in_order'"
$CLICKHOUSE_CLIENT -q "select '-- distinct all primary key columns -> ordinary distinct'"
$CLICKHOUSE_CLIENT -nq "$DISABLE_OPTIMIZATION;explain pipeline select distinct * from distinct_in_order_explain" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- enable optimize_distinct_in_order'"
$CLICKHOUSE_CLIENT -q "select '-- distinct with all primary key columns -> pre-distinct optimization only'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct * from distinct_in_order_explain" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with primary key prefix -> pre-distinct optimization only'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct a, c from distinct_in_order_explain" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with primary key prefix and order by column in distinct -> pre-distinct and final distinct optimization'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct a, c from distinct_in_order_explain order by c" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with primary key prefix and order by the same columns -> pre-distinct and final distinct optimization'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct a, b from distinct_in_order_explain order by a, b" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with primary key prefix and order by columns are prefix of distinct columns -> pre-distinct and final distinct optimization'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct a, b from distinct_in_order_explain order by a" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with primary key prefix and order by column in distinct but non-primary key prefix -> pre-distinct and final distinct optimization'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct a, b, c from distinct_in_order_explain order by c" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with primary key prefix and order by column _not_ in distinct -> pre-distinct optimization only'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct a, c from distinct_in_order_explain order by b" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with non-primary key prefix -> ordinary distinct'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct b, c from distinct_in_order_explain" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with non-primary key prefix and order by column in distinct -> final distinct optimization only'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct b, c from distinct_in_order_explain order by b" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with non-primary key prefix and order by column _not_ in distinct -> ordinary distinct'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct b, c from distinct_in_order_explain order by a" | eval $FIND_DISTINCT

$CLICKHOUSE_CLIENT -q "select '-- distinct with non-primary key prefix and order by _const_ column in distinct -> ordinary distinct'"
$CLICKHOUSE_CLIENT -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct b, 1 as x from distinct_in_order_explain order by x" | eval $FIND_DISTINCT

echo "-- Check reading in order for distinct"
echo "-- disabled, distinct columns match sorting key"
$CLICKHOUSE_CLIENT --max_threads=0 -nq "$DISABLE_OPTIMIZATION;explain pipeline select distinct a, b from distinct_in_order_explain" | eval $FIND_READING_DEFAULT
echo "-- enabled, distinct columns match sorting key"
# read_in_order_two_level_merge_threshold is set here to avoid repeating MergeTreeInOrder in output
$CLICKHOUSE_CLIENT --read_in_order_two_level_merge_threshold=2 -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct a, b from distinct_in_order_explain" | eval $FIND_READING_IN_ORDER
echo "-- enabled, distinct columns form prefix of sorting key"
$CLICKHOUSE_CLIENT --read_in_order_two_level_merge_threshold=2 -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct a, b from distinct_in_order_explain" | eval $FIND_READING_IN_ORDER
echo "-- enabled, distinct columns DON't form prefix of sorting key"
$CLICKHOUSE_CLIENT --max_threads=0 -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct b from distinct_in_order_explain" | eval $FIND_READING_DEFAULT
echo "-- enabled, distinct columns contains constant columns, non-const columns form prefix of sorting key"
$CLICKHOUSE_CLIENT --read_in_order_two_level_merge_threshold=2 -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct 1, a from distinct_in_order_explain" | eval $FIND_READING_IN_ORDER
echo "-- enabled, distinct columns contains constant columns, non-const columns match prefix of sorting key"
$CLICKHOUSE_CLIENT --read_in_order_two_level_merge_threshold=2 -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct 1, b, a from distinct_in_order_explain" | eval $FIND_READING_IN_ORDER
echo "-- enabled, only part of distinct columns form prefix of sorting key"
$CLICKHOUSE_CLIENT --max_threads=0 -nq "$ENABLE_OPTIMIZATION;explain pipeline select distinct a, c from distinct_in_order_explain" | eval $FIND_READING_DEFAULT

echo "=== disable new analyzer ==="
DISABLE_ANALYZER="set enable_analyzer=0"

echo "-- enabled, check that sorting properties are propagated from ReadFromMergeTree till preliminary distinct"
$CLICKHOUSE_CLIENT -nq "$DISABLE_ANALYZER;$ENABLE_OPTIMIZATION;explain plan sorting=1 select distinct b, a from distinct_in_order_explain where a > 0" | eval $FIND_SORTING_PROPERTIES

echo "-- check that reading in order optimization for ORDER BY and DISTINCT applied correctly in the same query"
ENABLE_READ_IN_ORDER="set optimize_read_in_order=1"
echo "-- disabled, check that sorting description for ReadFromMergeTree match ORDER BY columns"
$CLICKHOUSE_CLIENT -nq "$DISABLE_ANALYZER;$DISABLE_OPTIMIZATION;$ENABLE_READ_IN_ORDER;explain plan sorting=1 select distinct b, a from distinct_in_order_explain order by a" | eval $FIND_SORTING_PROPERTIES
echo "-- enabled, check that ReadFromMergeTree sorting description is overwritten by DISTINCT optimization i.e. it contains columns from DISTINCT clause"
$CLICKHOUSE_CLIENT -nq "$DISABLE_ANALYZER;$ENABLE_OPTIMIZATION;$ENABLE_READ_IN_ORDER;explain plan sorting=1 select distinct b, a from distinct_in_order_explain order by a" | eval $FIND_SORTING_PROPERTIES
echo "-- enabled, check that ReadFromMergeTree sorting description is overwritten by DISTINCT optimization, but direction used from ORDER BY clause"
$CLICKHOUSE_CLIENT -nq "$DISABLE_ANALYZER;$ENABLE_OPTIMIZATION;$ENABLE_READ_IN_ORDER;explain plan sorting=1 select distinct b, a from distinct_in_order_explain order by a DESC" | eval $FIND_SORTING_PROPERTIES
echo "-- enabled, check that ReadFromMergeTree sorting description is NOT overwritten by DISTINCT optimization (1), - it contains columns from ORDER BY clause"
$CLICKHOUSE_CLIENT -nq "$DISABLE_ANALYZER;$ENABLE_OPTIMIZATION;$ENABLE_READ_IN_ORDER;explain plan sorting=1 select distinct a from distinct_in_order_explain order by a, b" | eval $FIND_SORTING_PROPERTIES
echo "-- enabled, check that ReadFromMergeTree sorting description is NOT overwritten by DISTINCT optimization (2), - direction used from ORDER BY clause"
$CLICKHOUSE_CLIENT -nq "$DISABLE_ANALYZER;$ENABLE_OPTIMIZATION;$ENABLE_READ_IN_ORDER;explain plan sorting=1 select distinct b, a from distinct_in_order_explain order by a DESC, b DESC" | eval $FIND_SORTING_PROPERTIES

echo "-- enabled, check that disabling other 'read in order' optimizations do not disable distinct in order optimization"
$CLICKHOUSE_CLIENT -nq "$DISABLE_ANALYZER;$ENABLE_OPTIMIZATION;set optimize_read_in_order=0;set optimize_aggregation_in_order=0;set optimize_read_in_window_order=0;explain plan sorting=1 select distinct a,b from distinct_in_order_explain" | eval $FIND_SORTING_PROPERTIES

echo "=== enable new analyzer ==="
ENABLE_ANALYZER="set enable_analyzer=1"

echo "-- enabled, check that sorting properties are propagated from ReadFromMergeTree till preliminary distinct"
$CLICKHOUSE_CLIENT -nq "$ENABLE_ANALYZER;$ENABLE_OPTIMIZATION;explain plan sorting=1 select distinct b, a from distinct_in_order_explain where a > 0 settings optimize_move_to_prewhere=1" | eval $FIND_SORTING_PROPERTIES

echo "-- disabled, check that sorting description for ReadFromMergeTree match ORDER BY columns"
$CLICKHOUSE_CLIENT -nq "$ENABLE_ANALYZER;$DISABLE_OPTIMIZATION;$ENABLE_READ_IN_ORDER;explain plan sorting=1 select distinct b, a from distinct_in_order_explain order by a" | eval $FIND_SORTING_PROPERTIES
echo "-- enabled, check that ReadFromMergeTree sorting description is overwritten by DISTINCT optimization i.e. it contains columns from DISTINCT clause"
$CLICKHOUSE_CLIENT -nq "$ENABLE_ANALYZER;$ENABLE_OPTIMIZATION;$ENABLE_READ_IN_ORDER;explain plan sorting=1 select distinct b, a from distinct_in_order_explain order by a" | eval $FIND_SORTING_PROPERTIES
echo "-- enabled, check that ReadFromMergeTree sorting description is overwritten by DISTINCT optimization, but direction used from ORDER BY clause"
$CLICKHOUSE_CLIENT -nq "$ENABLE_ANALYZER;$ENABLE_OPTIMIZATION;$ENABLE_READ_IN_ORDER;explain plan sorting=1 select distinct b, a from distinct_in_order_explain order by a DESC" | eval $FIND_SORTING_PROPERTIES
echo "-- enabled, check that ReadFromMergeTree sorting description is NOT overwritten by DISTINCT optimization (1), - it contains columns from ORDER BY clause"
$CLICKHOUSE_CLIENT -nq "$ENABLE_ANALYZER;$ENABLE_OPTIMIZATION;$ENABLE_READ_IN_ORDER;explain plan sorting=1 select distinct a from distinct_in_order_explain order by a, b" | eval $FIND_SORTING_PROPERTIES
echo "-- enabled, check that ReadFromMergeTree sorting description is NOT overwritten by DISTINCT optimization (2), - direction used from ORDER BY clause"
$CLICKHOUSE_CLIENT -nq "$ENABLE_ANALYZER;$ENABLE_OPTIMIZATION;$ENABLE_READ_IN_ORDER;explain plan sorting=1 select distinct b, a from distinct_in_order_explain order by a DESC, b DESC" | eval $FIND_SORTING_PROPERTIES

echo "-- enabled, check that disabling other 'read in order' optimizations do not disable distinct in order optimization"
$CLICKHOUSE_CLIENT -nq "$ENABLE_ANALYZER;$ENABLE_OPTIMIZATION;set optimize_read_in_order=0;set optimize_aggregation_in_order=0;set optimize_read_in_window_order=0;explain plan sorting=1 select distinct a,b from distinct_in_order_explain" | eval $FIND_SORTING_PROPERTIES

$CLICKHOUSE_CLIENT -q "drop table if exists distinct_in_order_explain sync"

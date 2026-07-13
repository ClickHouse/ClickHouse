#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

# Test that the per-part `PrefetchingConcat` safeguards are also applied to child
# `ReadFromMergeTree` steps reached through a `Merge` table.
#
# A downstream aggregation-in-order or distinct-in-order benefits from multiple parallel
# input streams. For a direct `ReadFromMergeTree` read this is ensured by
# `setPreferMultipleStreams`, which disables the per-part `PrefetchingConcat` that would
# otherwise collapse the streams into one per part. The same must hold when the read goes
# through a `Merge` table: `ReadFromMerge::setPreferMultipleStreams` propagates the flag to
# the child readers, so `PrefetchingConcat` must NOT appear for these queries.
#
# The other safeguard is the outer `LIMIT`: when `ORDER BY ... LIMIT` reads in order through
# a `JOIN` (`query_plan_read_in_order_through_join`), the `LIMIT` cannot be pushed to the
# reader, so `ReadFromMergeTree::requestReadingInOrder` records it as `has_outer_limit`, which
# also disables per-part `PrefetchingConcat` (prefetching later ranges would defeat early
# termination on the outer `LIMIT`). `ReadFromMerge::requestReadingInOrder` propagates the
# same `query_limit`/`read_limit` to the `Merge` child readers, so `PrefetchingConcat` must
# NOT appear for the child reader of a `Merge` table under such an outer `LIMIT` either.
#
# `EXPLAIN PIPELINE` does not descend into the child pipeline of a `Merge` table, so we
# inspect the actually-built pipeline through `system.processors_profile_log`, correlating
# by explicit query ids.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_concat_merge_data;
DROP TABLE IF EXISTS t_concat_merge;
DROP TABLE IF EXISTS t_concat_merge_join_right;

CREATE TABLE t_concat_merge_data (key UInt64, value String)
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY key
SETTINGS index_granularity = 1024;
INSERT INTO t_concat_merge_data SELECT number, toString(number) FROM numbers(90000);
OPTIMIZE TABLE t_concat_merge_data FINAL;

CREATE TABLE t_concat_merge AS t_concat_merge_data ENGINE = Merge(currentDatabase(), '^t_concat_merge_data\$');

-- A small right table for the read-in-order-through-join case below. Its keys are a sparse
-- subset of the left keys, so a LEFT JOIN keeps every left row and does not multiply them.
CREATE TABLE t_concat_merge_join_right (key UInt64, tag String)
ENGINE = MergeTree ORDER BY key;
INSERT INTO t_concat_merge_join_right SELECT number * 1000, toString(number) FROM numbers(90);
"

SETTINGS="enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1, log_processors_profiles = 1,
          merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2"

QID_AGG="${CLICKHOUSE_DATABASE}_agg"
QID_DISTINCT="${CLICKHOUSE_DATABASE}_distinct"
QID_PLAIN="${CLICKHOUSE_DATABASE}_plain"
QID_JOIN="${CLICKHOUSE_DATABASE}_join"

# Aggregation-in-order over a `Merge` table on top of a multi-part table.
$CLICKHOUSE_CLIENT --query_id "$QID_AGG" --query \
    "SELECT key, count() FROM t_concat_merge GROUP BY key FORMAT Null SETTINGS $SETTINGS, optimize_aggregation_in_order = 1"

# Distinct-in-order over a `Merge` table on top of a multi-part table.
$CLICKHOUSE_CLIENT --query_id "$QID_DISTINCT" --query \
    "SELECT DISTINCT key FROM t_concat_merge ORDER BY key FORMAT Null SETTINGS $SETTINGS, optimize_distinct_in_order = 1"

# A plain read-in-order through the `Merge` table (no aggregation/distinct).
$CLICKHOUSE_CLIENT --query_id "$QID_PLAIN" --query \
    "SELECT * FROM t_concat_merge WHERE value LIKE '%5%' ORDER BY key FORMAT Null SETTINGS $SETTINGS"

# Read-in-order through a `JOIN` with an outer `LIMIT` over the `Merge` table. The outer
# `LIMIT` cannot be pushed to the reader through the `LEFT JOIN`, so it becomes `has_outer_limit`
# on the `Merge` child readers, which must keep per-part `PrefetchingConcat` disabled.
$CLICKHOUSE_CLIENT --query_id "$QID_JOIN" --query \
    "SELECT t.key, r.tag FROM t_concat_merge AS t LEFT JOIN t_concat_merge_join_right AS r ON t.key = r.key
     ORDER BY t.key LIMIT 10 FORMAT Null SETTINGS $SETTINGS, query_plan_read_in_order_through_join = 1, enable_analyzer = 1"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS processors_profile_log"

# Aggregation-in-order and distinct-in-order must NOT collapse streams with per-part
# `PrefetchingConcat`. A plain read-in-order still uses it per part.
$CLICKHOUSE_CLIENT --query "
SELECT 'agg_in_order_no_prefetching_merge', countIf(name = 'PrefetchingConcat') = 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_AGG';
SELECT 'distinct_in_order_no_prefetching_merge', countIf(name = 'PrefetchingConcat') = 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_DISTINCT';
SELECT 'plain_read_in_order_prefetching_merge', countIf(name = 'PrefetchingConcat') > 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_PLAIN';
-- The JOIN case still reads the Merge child in order (guards against a vacuous pass) ...
SELECT 'join_outer_limit_reads_in_order', countIf(name LIKE '%algorithm: InOrder%') > 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_JOIN';
-- ... but the outer LIMIT (has_outer_limit) keeps per-part PrefetchingConcat disabled.
SELECT 'join_outer_limit_no_prefetching_merge', countIf(name = 'PrefetchingConcat') = 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_JOIN';
"

# Correctness: aggregation and distinct produce the expected results.
$CLICKHOUSE_CLIENT --query "
SELECT 'correctness';
SELECT sum(key), count() FROM (SELECT key, count() AS c FROM t_concat_merge GROUP BY key SETTINGS optimize_aggregation_in_order = 1);
SELECT groupArray(key) = arraySort(groupArray(key)) FROM (SELECT DISTINCT key FROM t_concat_merge ORDER BY key SETTINGS optimize_distinct_in_order = 1);
SELECT 'join_correctness';
-- Keep the settings on the top-level query (not the subquery): 'enable_analyzer' cannot be
-- changed inside a subquery when the top-level value differs (INCORRECT_QUERY), which happens
-- under the old-analyzer configuration where the default is 0. Subqueries inherit these.
SELECT arraySort(groupArray(key)) FROM (
    SELECT t.key AS key FROM t_concat_merge AS t LEFT JOIN t_concat_merge_join_right AS r ON t.key = r.key
    ORDER BY t.key LIMIT 10) SETTINGS query_plan_read_in_order_through_join = 1, enable_analyzer = 1;
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_concat_merge; DROP TABLE t_concat_merge_data; DROP TABLE t_concat_merge_join_right;"

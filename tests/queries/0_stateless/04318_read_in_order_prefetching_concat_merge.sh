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
# `EXPLAIN PIPELINE` does not descend into the child pipeline of a `Merge` table, so we
# inspect the actually-built pipeline through `system.processors_profile_log`, correlating
# by explicit query ids.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_concat_merge_data;
DROP TABLE IF EXISTS t_concat_merge;

CREATE TABLE t_concat_merge_data (key UInt64, value String)
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY key
SETTINGS index_granularity = 1024;
INSERT INTO t_concat_merge_data SELECT number, toString(number) FROM numbers(90000);
OPTIMIZE TABLE t_concat_merge_data FINAL;

CREATE TABLE t_concat_merge AS t_concat_merge_data ENGINE = Merge(currentDatabase(), '^t_concat_merge_data\$');
"

SETTINGS="enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1, log_processors_profiles = 1,
          merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2"

QID_AGG="${CLICKHOUSE_DATABASE}_agg"
QID_DISTINCT="${CLICKHOUSE_DATABASE}_distinct"
QID_PLAIN="${CLICKHOUSE_DATABASE}_plain"

# Aggregation-in-order over a `Merge` table on top of a multi-part table.
$CLICKHOUSE_CLIENT --query_id "$QID_AGG" --query \
    "SELECT key, count() FROM t_concat_merge GROUP BY key FORMAT Null SETTINGS $SETTINGS, optimize_aggregation_in_order = 1"

# Distinct-in-order over a `Merge` table on top of a multi-part table.
$CLICKHOUSE_CLIENT --query_id "$QID_DISTINCT" --query \
    "SELECT DISTINCT key FROM t_concat_merge ORDER BY key FORMAT Null SETTINGS $SETTINGS, optimize_distinct_in_order = 1"

# A plain read-in-order through the `Merge` table (no aggregation/distinct).
$CLICKHOUSE_CLIENT --query_id "$QID_PLAIN" --query \
    "SELECT * FROM t_concat_merge WHERE value LIKE '%5%' ORDER BY key FORMAT Null SETTINGS $SETTINGS"

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
"

# Correctness: aggregation and distinct produce the expected results.
$CLICKHOUSE_CLIENT --query "
SELECT 'correctness';
SELECT sum(key), count() FROM (SELECT key, count() AS c FROM t_concat_merge GROUP BY key SETTINGS optimize_aggregation_in_order = 1);
SELECT groupArray(key) = arraySort(groupArray(key)) FROM (SELECT DISTINCT key FROM t_concat_merge ORDER BY key SETTINGS optimize_distinct_in_order = 1);
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_concat_merge; DROP TABLE t_concat_merge_data;"

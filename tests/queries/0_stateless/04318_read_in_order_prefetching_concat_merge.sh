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
DROP TABLE IF EXISTS t_concat_merge_nested;
DROP TABLE IF EXISTS t_concat_merge_join_right;
DROP TABLE IF EXISTS t_concat_merge_lb_data;
DROP TABLE IF EXISTS t_concat_merge_lb;

CREATE TABLE t_concat_merge_data (key UInt64, value String)
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY key
SETTINGS index_granularity = 1024;
INSERT INTO t_concat_merge_data SELECT number, toString(number) FROM numbers(90000);
OPTIMIZE TABLE t_concat_merge_data FINAL;

CREATE TABLE t_concat_merge AS t_concat_merge_data ENGINE = Merge(currentDatabase(), '^t_concat_merge_data\$');

-- A multi-part table with a low-cardinality leading sort column, for the \`LIMIT BY\` case
-- below. \`LIMIT BY grp\` drives read-in-order (\`grp\` is a prefix of the sorting key), so it
-- must exercise the \`ReadFromMerge\` \`LIMIT BY\` branch that propagates \`setPreferMultipleStreams\`.
CREATE TABLE t_concat_merge_lb_data (grp UInt64, key UInt64, value String)
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY (grp, key)
SETTINGS index_granularity = 1024;
INSERT INTO t_concat_merge_lb_data SELECT number % 100, number, toString(number) FROM numbers(90000);
OPTIMIZE TABLE t_concat_merge_lb_data FINAL;

CREATE TABLE t_concat_merge_lb AS t_concat_merge_lb_data ENGINE = Merge(currentDatabase(), '^t_concat_merge_lb_data\$');

-- A nested \`Merge\` table: its only child is itself a \`Merge\` table. The child readers of the
-- inner \`Merge\` live in the inner step's internal child plans, so the safeguards must be
-- propagated through the nested \`ReadFromMerge\` step, not only to direct \`ReadFromMergeTree\` ones.
CREATE TABLE t_concat_merge_nested AS t_concat_merge_data ENGINE = Merge(currentDatabase(), '^t_concat_merge\$');

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
QID_VROW="${CLICKHOUSE_DATABASE}_vrow"
QID_AGG_NESTED="${CLICKHOUSE_DATABASE}_agg_nested"
QID_PLAIN_NESTED="${CLICKHOUSE_DATABASE}_plain_nested"
QID_JOIN_NESTED="${CLICKHOUSE_DATABASE}_join_nested"
QID_LIMIT_BY="${CLICKHOUSE_DATABASE}_limit_by"

# Aggregation-in-order over a `Merge` table on top of a multi-part table.
$CLICKHOUSE_CLIENT --query_id "$QID_AGG" --query \
    "SELECT key, count() FROM t_concat_merge GROUP BY key FORMAT Null SETTINGS $SETTINGS, optimize_aggregation_in_order = 1"

# Distinct-in-order over a `Merge` table on top of a multi-part table.
$CLICKHOUSE_CLIENT --query_id "$QID_DISTINCT" --query \
    "SELECT DISTINCT key FROM t_concat_merge ORDER BY key FORMAT Null SETTINGS $SETTINGS, optimize_distinct_in_order = 1"

# A plain read-in-order through the `Merge` table (no aggregation/distinct).
$CLICKHOUSE_CLIENT --query_id "$QID_PLAIN" --query \
    "SELECT * FROM t_concat_merge WHERE value LIKE '%5%' ORDER BY key FORMAT Null SETTINGS $SETTINGS"

# A direct multi-part read-in-order with per-block virtual rows enabled. Per-block virtual rows
# require `MergingSortedTransform` to observe a block's virtual row before any later real chunk
# from that source is read, and `PrefetchingConcatProcessor` cannot honor that (it pulls eagerly
# from all inputs without virtual-row stop logic). So per-part `PrefetchingConcat` must NOT appear
# in this mode; the read still goes in order (via `VirtualRowTransform`). We read the underlying
# table directly here because virtual rows are only emitted on the direct `MergeTree` read path.
$CLICKHOUSE_CLIENT --query_id "$QID_VROW" --query \
    "SELECT * FROM t_concat_merge_data WHERE value LIKE '%5%' ORDER BY key FORMAT Null
     SETTINGS $SETTINGS, read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1"

# Read-in-order through a `JOIN` with an outer `LIMIT` over the `Merge` table. The outer
# `LIMIT` cannot be pushed to the reader through the `LEFT JOIN`, so it becomes `has_outer_limit`
# on the `Merge` child readers, which must keep per-part `PrefetchingConcat` disabled.
$CLICKHOUSE_CLIENT --query_id "$QID_JOIN" --query \
    "SELECT t.key, r.tag FROM t_concat_merge AS t LEFT JOIN t_concat_merge_join_right AS r ON t.key = r.key
     ORDER BY t.key LIMIT 10 FORMAT Null SETTINGS $SETTINGS, query_plan_read_in_order_through_join = 1, enable_analyzer = 1"

# `LIMIT BY` through the `Merge` table drives read-in-order without an outer `ORDER BY`:
# `LimitByStep` runs a per-stream `LimitBySortedStreamTransform` prefilter and merges the
# streams, so it benefits from multiple parallel input streams. The read-in-order input order
# carries `limit = 0`, so per-part `PrefetchingConcat` would otherwise be taken on the multi-part
# child reader and collapse the parallel prefilter into one stream per part.
# `ReadFromMerge::setPreferMultipleStreams` propagates the flag to the child readers, so
# `PrefetchingConcat` must NOT appear, while the read still goes in order (proven by the
# streaming `LimitBySortedStreamTransform`).
$CLICKHOUSE_CLIENT --query_id "$QID_LIMIT_BY" --query \
    "SELECT * FROM t_concat_merge_lb LIMIT 3 BY grp FORMAT Null SETTINGS $SETTINGS"

# The same shapes through a nested `Merge` table (a `Merge` whose child is a `Merge`).
# The safeguards are propagated through the nested `ReadFromMerge` step to the inner readers
# (`recursivelyApplyToReadingSteps` descends into nested `ReadFromMerge` child plans).
#
# NOTE: today the read-in-order optimization does not engage for a nested `Merge` at all:
# the reading-step discovery requires every selected table to have a non-empty sorting key,
# and a `Merge` table's metadata has none, so the outer `ReadFromMerge` is rejected before
# `requestReadingInOrder` / `setPreferMultipleStreams` are ever called. The checks below pin
# down that behavior as canaries: if nested `Merge` ever starts reading in order, they flip,
# and the safeguard propagation (already recursive) must be re-verified with positive controls.
$CLICKHOUSE_CLIENT --query_id "$QID_AGG_NESTED" --query \
    "SELECT key, count() FROM t_concat_merge_nested GROUP BY key FORMAT Null SETTINGS $SETTINGS, optimize_aggregation_in_order = 1"

$CLICKHOUSE_CLIENT --query_id "$QID_PLAIN_NESTED" --query \
    "SELECT * FROM t_concat_merge_nested WHERE value LIKE '%5%' ORDER BY key FORMAT Null SETTINGS $SETTINGS"

$CLICKHOUSE_CLIENT --query_id "$QID_JOIN_NESTED" --query \
    "SELECT t.key, r.tag FROM t_concat_merge_nested AS t LEFT JOIN t_concat_merge_join_right AS r ON t.key = r.key
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
-- With per-block virtual rows the read still goes in order (guards against a vacuous pass) ...
SELECT 'virtual_row_per_block_reads_in_order', countIf(name LIKE '%algorithm: InOrder%') > 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_VROW';
-- ... but per-part PrefetchingConcat is disabled to preserve the virtual-row boundary contract.
SELECT 'virtual_row_per_block_no_prefetching', countIf(name = 'PrefetchingConcat') = 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_VROW';
-- The JOIN case still reads the Merge child in order (guards against a vacuous pass) ...
SELECT 'join_outer_limit_reads_in_order', countIf(name LIKE '%algorithm: InOrder%') > 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_JOIN';
-- ... but the outer LIMIT (has_outer_limit) keeps per-part PrefetchingConcat disabled.
SELECT 'join_outer_limit_no_prefetching_merge', countIf(name = 'PrefetchingConcat') = 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_JOIN';
-- Nested Merge canary: today the read-in-order optimization does not engage through a nested
-- Merge (see the note above). If this flips to reading in order, re-verify the safeguard
-- propagation with positive controls instead of these canaries.
SELECT 'plain_nested_merge_not_read_in_order_yet', countIf(name LIKE '%algorithm: InOrder%') = 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_PLAIN_NESTED';
-- Nested Merge: per-part PrefetchingConcat must not appear on the inner readers in any of
-- these shapes (today because no read-in-order engages at all; with future nested in-order
-- support, because the safeguards are propagated through the nested ReadFromMerge step).
SELECT 'agg_in_order_no_prefetching_nested_merge', countIf(name = 'PrefetchingConcat') = 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_AGG_NESTED';
SELECT 'plain_no_prefetching_nested_merge', countIf(name = 'PrefetchingConcat') = 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_PLAIN_NESTED';
SELECT 'join_outer_limit_no_prefetching_nested_merge', countIf(name = 'PrefetchingConcat') = 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_JOIN_NESTED';
-- LIMIT BY through the Merge table still reads the child in order (guards against a vacuous pass) ...
SELECT 'limit_by_reads_in_order_merge', countIf(name = 'LimitBySortedStreamTransform') > 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_LIMIT_BY';
-- ... but setPreferMultipleStreams keeps per-part PrefetchingConcat disabled on the child reader.
SELECT 'limit_by_no_prefetching_merge', countIf(name = 'PrefetchingConcat') = 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_LIMIT_BY';
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
SELECT 'nested_correctness';
SELECT sum(key), count() FROM (SELECT key, count() AS c FROM t_concat_merge_nested GROUP BY key SETTINGS optimize_aggregation_in_order = 1);
SELECT groupArray(key) = arraySort(groupArray(key)) FROM (SELECT key FROM t_concat_merge_nested ORDER BY key);
SELECT arraySort(groupArray(key)) FROM (
    SELECT t.key AS key FROM t_concat_merge_nested AS t LEFT JOIN t_concat_merge_join_right AS r ON t.key = r.key
    ORDER BY t.key LIMIT 10) SETTINGS query_plan_read_in_order_through_join = 1, enable_analyzer = 1;
SELECT 'limit_by_correctness';
-- LIMIT BY without an outer ORDER BY does not fix which 3 rows per group are kept, so we assert
-- order-independent invariants: exactly 3 rows per group, all 100 groups present, and every
-- returned row is self-consistent (key % 100 is the group it was placed in).
SELECT count() = 300 AND uniqExact(grp) = 100 AND countIf(key % 100 != grp) = 0 FROM (SELECT grp, key FROM t_concat_merge_lb LIMIT 3 BY grp);
SELECT min(c) = 3 AND max(c) = 3 FROM (SELECT count() AS c FROM (SELECT grp, key FROM t_concat_merge_lb LIMIT 3 BY grp) GROUP BY grp);
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_concat_merge_nested; DROP TABLE t_concat_merge; DROP TABLE t_concat_merge_data; DROP TABLE t_concat_merge_join_right; DROP TABLE t_concat_merge_lb; DROP TABLE t_concat_merge_lb_data;"

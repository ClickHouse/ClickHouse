#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas, no-object-storage

# Regression test: a read-in-order `ORDER BY prefix, suffix LIMIT n` query goes through
# `SortingStep::transformPipeline`'s `Type::FinishSorting` branch, which merges by
# `prefix_description` first (deliberately with `limit_ = 0`, since the true top-`n` rows
# are only known after `finishSorting` re-sorts by `suffix`) and then applies `FinishSorting`.
#
# `mergingSorted` decides whether to build the hierarchical merge tree from whether the step
# has a limit at all - not from whether the *local* `limit_` argument it was called with is
# zero. Using the local argument as the signal (as an earlier version of this feature did)
# would build a hierarchical merge for this prefix merge, even though the whole `SortingStep`
# has `LIMIT n`. That reintroduces the over-read regression the direct-merge case was fixed
# for: each intermediate `MergingSortedTransform` would pull its own group's frontier forward
# before the global top-n frontier is known.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `grep -c "MergingSortedTransform ×"` is 1 only when a layer of parallel mergers exists.
has_hierarchy() { grep -c "MergingSortedTransform ×"; }

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_finish_sorting_limit"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_finish_sorting_limit (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192"
# Stop merges so the number of parts (and therefore read streams) is stable.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_finish_sorting_limit"
# 20 small parts: more parts than the threshold of 16, and small enough that the test
# stays fast under sanitizers (the sibling `04492_*` test used to time out in the TSan
# flaky check with bigger parts). The stream count depends on the number of parts, not
# on their size, so the hierarchy decision is unaffected.
$CLICKHOUSE_CLIENT -q "
    $(for _ in $(seq 1 20); do echo "INSERT INTO t_finish_sorting_limit SELECT number, number FROM numbers(10000);"; done)"

FS_SETTINGS="optimize_read_in_order = 1, read_in_order_two_level_merge_threshold = 1,
    max_threads = 32, max_threads_min_free_memory_per_thread = 0, max_rows_to_read = 0"

echo '-- FinishSorting, ORDER BY prefix, suffix LIMIT n: no hierarchy over 20 parts (0) --'
$CLICKHOUSE_CLIENT -q "EXPLAIN PIPELINE SELECT a, b FROM t_finish_sorting_limit ORDER BY a, b LIMIT 10
    SETTINGS $FS_SETTINGS, max_streams_per_hierarchical_merge = 16" | has_hierarchy

echo '-- Same, without LIMIT: hierarchy over 20 parts (1) --'
$CLICKHOUSE_CLIENT -q "EXPLAIN PIPELINE SELECT a, b FROM t_finish_sorting_limit ORDER BY a, b
    SETTINGS $FS_SETTINGS, max_streams_per_hierarchical_merge = 16" | has_hierarchy

echo '-- LIMIT results are identical whether the hierarchy is enabled or disabled --'
WITH_HIERARCHY=$($CLICKHOUSE_CLIENT -q "SELECT groupArray((a, b)) FROM (SELECT a, b FROM t_finish_sorting_limit ORDER BY a, b LIMIT 10
    SETTINGS $FS_SETTINGS, max_streams_per_hierarchical_merge = 16)")
WITHOUT_HIERARCHY=$($CLICKHOUSE_CLIENT -q "SELECT groupArray((a, b)) FROM (SELECT a, b FROM t_finish_sorting_limit ORDER BY a, b LIMIT 10
    SETTINGS $FS_SETTINGS, max_streams_per_hierarchical_merge = 0)")
[ "$WITH_HIERARCHY" = "$WITHOUT_HIERARCHY" ] && echo 1 || echo 0

$CLICKHOUSE_CLIENT -q "DROP TABLE t_finish_sorting_limit"

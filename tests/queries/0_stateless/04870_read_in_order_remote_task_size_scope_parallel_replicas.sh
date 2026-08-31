#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
# no-fasttest: needs the `s3_cache` storage policy - the task-size heuristic under test only runs
# for parts stored on remote disks.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Same contract as 04812_read_in_order_stream_budget_scope_union_view_parallel_replicas and
# 04826_read_in_order_concurrent_read_scope_union_view_parallel_replicas, for the remote-reading
# task-size pair: `MergeTreeReadPoolBase::calculateMinMarksPerTask` consults
# `merge_tree_min_bytes_per_task_for_remote_reading` (and `merge_tree_determine_task_size_by_prewhere_columns`)
# for parts stored on remote disks, and the derived `min_marks_per_task` is written by the snapshot
# replica into the coordinator's first announcement and reused by the followers. The view branch is
# planned under its own `SETTINGS`, so if the initiator-local rebuild reads the pair from the *outer*
# context instead (`makeShippedFragmentReadingContext` must copy it), the initiator-local fragment
# announces a task size derived from the outer value.
#
# The branch lowers `merge_tree_min_bytes_per_task_for_remote_reading` to 1 byte, so the heuristic
# must never raise `min_marks_per_task` above the floor (the concurrent-read floors are pinned to one
# mark). The outer value is raised to 100 GB: under the leak the initiator-local pool would jump to
# `sum_marks / (threads * replicas) / 2` (~80 marks for the ~1950 marks of this table, 4 threads, 3 replicas).

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t_remote_task_size;
DROP VIEW IF EXISTS v_remote_task_size;

CREATE TABLE t_remote_task_size (key UInt64)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 256, storage_policy = 's3_cache';

SYSTEM STOP MERGES t_remote_task_size;

INSERT INTO t_remote_task_size SELECT number FROM numbers(500000);

CREATE VIEW v_remote_task_size AS
SELECT key FROM t_remote_task_size
SETTINGS merge_tree_min_bytes_per_task_for_remote_reading = 1;
"

SETTINGS="--enable_analyzer=1 --automatic_parallel_replicas_mode=0 --enable_parallel_replicas=1 \
--parallel_replicas_for_non_replicated_merge_tree=1 --max_parallel_replicas=3 \
--cluster_for_parallel_replicas=test_cluster_one_shard_three_replicas_localhost \
--parallel_replicas_local_plan=1 --parallel_replicas_allow_view_over_mergetree=1 \
--optimize_read_in_order=1 --max_threads=4 \
--merge_tree_min_rows_for_concurrent_read_for_remote_filesystem=1 \
--merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem=1 \
--merge_tree_min_read_task_size=1 \
--merge_tree_min_bytes_per_task_for_remote_reading=100000000000"

# `Will use min_marks_per_task` is logged at the `test` level by every read pool under this query -
# the initiator-local fragment's pool and the (same-server) remote replicas' pools alike.
CLICKHOUSE_CLIENT_TEST_LOGS=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=test/g')
LOG=$(${CLICKHOUSE_CLIENT_TEST_LOGS} ${SETTINGS} \
    -q "SELECT key FROM v_remote_task_size ORDER BY key FORMAT Null" 2>&1)

# The assertion is threshold-based, not exact, because the floor depends on the granularity of the
# index and the leaked value on the exact mark count: every pool must stay at the branch-derived
# floor (a few marks); a single oversized value means some pool derived the task size from the
# outer 100 GB.
echo "$LOG" | grep -o 'Will use min_marks_per_task=[0-9]*' | awk -F= '
    { seen = 1; if ($2 > 24) oversized++ }
    END { print "task_size_logged " (seen ? 1 : 0); print "oversized_task_sizes " (oversized + 0) }'

# Whatever the fragment is built under, the answer must not change and must stay ordered.
$CLICKHOUSE_CLIENT ${SETTINGS} -q "
SELECT count(), sum(key), groupArray(key) = arraySort(groupArray(key))
FROM (SELECT key FROM v_remote_task_size ORDER BY key);
"

$CLICKHOUSE_CLIENT -n -q "
DROP VIEW v_remote_task_size;
DROP TABLE t_remote_task_size;
"

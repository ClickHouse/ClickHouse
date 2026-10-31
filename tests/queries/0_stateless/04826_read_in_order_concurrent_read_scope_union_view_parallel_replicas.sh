#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Same contract as 04812_read_in_order_stream_budget_scope_union_view_parallel_replicas, for the
# `merge_tree_min_*_for_concurrent_read` family: those settings (with their `_for_remote_filesystem`
# variants and `merge_tree_min_read_task_size`) derive `PartRangesReadInfo::min_marks_for_concurrent_read`,
# which `spreadMarkRangesAmongStreamsWithOrder` uses both to shrink the global stream count and to cap
# the per-part split count. With `parallel_replicas_allow_view_over_mergetree` each `UNION ALL` view
# branch is planned under its own `SETTINGS`, so if the initiator-local rebuild reads the family from the
# *outer* context instead (`makeShippedFragmentReadingContext` must copy it), the initiator and the remote
# replicas pick different in-order `#split_i` topologies for the same shipped fragment, and follower
# streams unknown to the coordinator are dropped.
#
# As in 04812, the split topology is invisible in `EXPLAIN PIPELINE`, so the test pins the coordinator's
# stream registrations from the debug log: one part of ~30 marks with 4 planned streams and a branch-lowered
# threshold of 1 mark must produce 4 `#split_i` streams; under the leaked outer threshold (raised so one
# stream covers the part) it produced only 1.

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t_concurrent_read_capped;
DROP TABLE IF EXISTS t_concurrent_read_wide;
DROP VIEW IF EXISTS v_concurrent_read;

CREATE TABLE t_concurrent_read_capped (key UInt64, value String)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;
CREATE TABLE t_concurrent_read_wide (key UInt64, value String)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;

SYSTEM STOP MERGES t_concurrent_read_capped;
SYSTEM STOP MERGES t_concurrent_read_wide;

INSERT INTO t_concurrent_read_capped SELECT number, toString(number) FROM numbers(30000);
INSERT INTO t_concurrent_read_wide SELECT number, toString(number) FROM numbers(30000, 30000);

CREATE VIEW v_concurrent_read AS
SELECT key FROM t_concurrent_read_capped
SETTINGS merge_tree_min_rows_for_concurrent_read = 100000000,
         merge_tree_min_bytes_for_concurrent_read = 100000000,
         merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 100000000,
         merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 100000000,
         merge_tree_min_read_task_size = 1
UNION ALL
SELECT key FROM t_concurrent_read_wide
SETTINGS merge_tree_min_rows_for_concurrent_read = 1,
         merge_tree_min_bytes_for_concurrent_read = 1,
         merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 1,
         merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 1,
         merge_tree_min_read_task_size = 1;
"

# The outer values are raised so that under the leak one stream covers the whole part; the wide branch
# lowers them so its part splits into all 4 planned streams.
SETTINGS="--enable_analyzer=1 --automatic_parallel_replicas_mode=0 --enable_parallel_replicas=1 \
--parallel_replicas_for_non_replicated_merge_tree=1 --max_parallel_replicas=3 \
--cluster_for_parallel_replicas=test_cluster_one_shard_three_replicas_localhost \
--parallel_replicas_local_plan=1 --parallel_replicas_allow_view_over_mergetree=1 \
--optimize_read_in_order=1 --max_threads=4 \
--merge_tree_min_rows_for_concurrent_read=100000000 --merge_tree_min_bytes_for_concurrent_read=100000000 \
--merge_tree_min_rows_for_concurrent_read_for_remote_filesystem=100000000 \
--merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem=100000000 \
--merge_tree_min_read_task_size=1"

# The coordinator lives on the initiator, so its stream registrations are logged under this query.
CLICKHOUSE_CLIENT_DEBUG_LOGS=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=debug/g')
LOG=$(${CLICKHOUSE_CLIENT_DEBUG_LOGS} ${SETTINGS} \
    -q "SELECT key FROM v_concurrent_read ORDER BY key FORMAT Null" 2>&1)

# The branch that lowers the threshold must split by its own value (1 part, ~30 marks, 4 planned
# streams -> 4 splits), not stay at the outer-derived single stream. The branch that raises it must stay at 1.
echo "wide_branch_splits $(echo "$LOG" | grep -c "Created coordinator for stream ${CLICKHOUSE_DATABASE}.t_concurrent_read_wide#split_")"
echo "capped_branch_splits $(echo "$LOG" | grep -c "Created coordinator for stream ${CLICKHOUSE_DATABASE}.t_concurrent_read_capped#split_")"

# Whatever the fragment is built under, the answer must not change and must stay ordered.
$CLICKHOUSE_CLIENT ${SETTINGS} -q "
SELECT count(), sum(key), groupArray(key) = arraySort(groupArray(key))
FROM (SELECT key FROM v_concurrent_read ORDER BY key);
"

$CLICKHOUSE_CLIENT -n -q "
DROP VIEW v_concurrent_read;
DROP TABLE t_concurrent_read_wide;
DROP TABLE t_concurrent_read_capped;
"

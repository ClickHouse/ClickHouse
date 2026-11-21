#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The initiator-local fragment of a parallel-replicas read is rebuilt through the
# `ReadFromMergeTree` constructor, and that constructor re-derives the stream budget from the
# supplied context: it clamps `requested_num_streams` by `max_streams_for_merge_tree_reading`.
# With `parallel_replicas_allow_view_over_mergetree` a view expands into a `UNION ALL` whose
# branches carry their own `SETTINGS`, and each branch's read is planned (and re-planned on the
# remote replicas) under the branch context. If the rebuild clamps by the *outer* value instead,
# the initiator creates fewer in-order `#split_i` streams than the remote replicas announce, and
# since only the snapshot replica may introduce stream ids, the extra follower streams are dropped
# as unknown. `makeShippedFragmentReadingContext` must therefore carry the branch's stream-budget
# settings onto the rebuilt step, like the other read-in-order runtime settings.
#
# The split topology is not visible in `EXPLAIN PIPELINE` (sources are per part regardless of the
# split count), so the test pins the coordinator's own stream registrations from the debug log:
# with 3 equal parts and 4 planned streams the branch must produce 3 `#split_i` streams; under the
# leaked outer cap of 2 it produced only 2.

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t_stream_budget_capped;
DROP TABLE IF EXISTS t_stream_budget_wide;
DROP VIEW IF EXISTS v_stream_budget;

CREATE TABLE t_stream_budget_capped (key UInt64, value String)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;
CREATE TABLE t_stream_budget_wide (key UInt64, value String)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;

SYSTEM STOP MERGES t_stream_budget_capped;
SYSTEM STOP MERGES t_stream_budget_wide;

INSERT INTO t_stream_budget_capped SELECT number, toString(number) FROM numbers(30000);
INSERT INTO t_stream_budget_capped SELECT number, toString(number) FROM numbers(30000, 30000);
INSERT INTO t_stream_budget_capped SELECT number, toString(number) FROM numbers(60000, 30000);

INSERT INTO t_stream_budget_wide SELECT number, toString(number) FROM numbers(90000, 30000);
INSERT INTO t_stream_budget_wide SELECT number, toString(number) FROM numbers(120000, 30000);
INSERT INTO t_stream_budget_wide SELECT number, toString(number) FROM numbers(150000, 30000);

CREATE VIEW v_stream_budget AS
SELECT key FROM t_stream_budget_capped SETTINGS max_streams_for_merge_tree_reading = 1
UNION ALL
SELECT key FROM t_stream_budget_wide SETTINGS max_streams_for_merge_tree_reading = 16;
"

SETTINGS="--enable_analyzer=1 --automatic_parallel_replicas_mode=0 --enable_parallel_replicas=1 \
--parallel_replicas_for_non_replicated_merge_tree=1 --max_parallel_replicas=3 \
--cluster_for_parallel_replicas=test_cluster_one_shard_three_replicas_localhost \
--parallel_replicas_local_plan=1 --parallel_replicas_allow_view_over_mergetree=1 \
--optimize_read_in_order=1 --max_threads=4 --max_streams_for_merge_tree_reading=2"

# The coordinator lives on the initiator, so its stream registrations are logged under this query.
CLICKHOUSE_CLIENT_DEBUG_LOGS=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=debug/g')
LOG=$(${CLICKHOUSE_CLIENT_DEBUG_LOGS} ${SETTINGS} \
    -q "SELECT key FROM v_stream_budget ORDER BY key FORMAT Null" 2>&1)

# The branch that raises the budget must split by its own value (3 parts -> 3 splits with 4
# planned streams), not by the outer cap of 2. The branch that lowers it must stay at 1.
echo "wide_branch_splits $(echo "$LOG" | grep -c "Created coordinator for stream ${CLICKHOUSE_DATABASE}.t_stream_budget_wide#split_")"
echo "capped_branch_splits $(echo "$LOG" | grep -c "Created coordinator for stream ${CLICKHOUSE_DATABASE}.t_stream_budget_capped#split_")"

# Whatever the fragment is built under, the answer must not change and must stay ordered.
$CLICKHOUSE_CLIENT ${SETTINGS} -q "
SELECT count(), sum(key), groupArray(key) = arraySort(groupArray(key))
FROM (SELECT key FROM v_stream_budget ORDER BY key);
"

$CLICKHOUSE_CLIENT -n -q "
DROP VIEW v_stream_budget;
DROP TABLE t_stream_budget_wide;
DROP TABLE t_stream_budget_capped;
"

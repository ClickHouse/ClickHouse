#!/usr/bin/env bash

# Checks that a self-join, whose two reads share a key, is not declined at a remembered boundary: the remembered
# key of the coordinated read matches both of them, so it cannot say which one parallel replicas coordinate, and
# the plan with parallel replicas is built to name it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_sj;
    CREATE TABLE t_sj (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_sj SELECT number FROM numbers(1000000);
"

SETTINGS="
    enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_mode = 1, automatic_parallel_replicas_remember_boundary = 1,
    automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0,
    query_plan_join_swap_table = 0, enable_join_runtime_filters = 0,
    query_plan_optimize_join_order_randomize = 0"
QUERY="SELECT sum(a.id + b.id) FROM t_sj AS a INNER JOIN t_sj AS b ON a.id = b.id FORMAT Null"

# Remembers the boundary and the coordinated read, building the plan with parallel replicas to find them.
$CLICKHOUSE_CLIENT -q "$QUERY SETTINGS $SETTINGS, log_comment = 'sj_1_remember'"
# Both reads match the remembered one, so the plan is built again.
CLICKHOUSE_CLIENT_DEBUG=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=trace/g')
$CLICKHOUSE_CLIENT_DEBUG -q "$QUERY SETTINGS $SETTINGS, log_comment = 'sj_2_ambiguous'" 2>&1 \
    | grep -o "2 reads under the remembered boundary match the remembered coordinated read" | sort -u

$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT
        log_comment,
        ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] > 0 AS built_the_plan,
        ProfileEvents['AutomaticParallelReplicasDecidedWithoutBuildingPlan'] > 0 AS decided_without_building_the_plan
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
      AND current_database = currentDatabase() AND type = 'QueryFinish'
      -- \`sj_\` cannot collide with the comment the test harness sets, which starts with the test name
      AND log_comment LIKE 'sj_%'
      -- the replicas run sub-queries under the same comment; keep only the initiating query
      AND query_id = initial_query_id
    ORDER BY log_comment;
    DROP TABLE t_sj;
"

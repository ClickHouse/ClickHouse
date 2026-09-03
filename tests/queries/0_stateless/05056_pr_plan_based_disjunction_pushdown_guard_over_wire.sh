#!/usr/bin/env bash
# Tags: no-old-analyzer
# The old analyzer has neither the plan-based parallel-replicas path nor `JoinStepLogical`.

# The plan-based parallel-replicas path ships an already-optimized plan to the replicas, which optimize
# it again before building the pipeline. `JoinStepLogical` used to drop `disjunctions_optimization_applied`
# on the wire, so the disjunction push-down ran a second time on every remote fragment and added a
# partial predicate the read already applies through PREWHERE. The replicas must not push anything.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE disj_wire_left (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO disj_wire_left SELECT number, if(number % 2 = 0, 'FRANCE', 'GERMANY') FROM numbers(1000);
    CREATE TABLE disj_wire_right (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO disj_wire_right SELECT number, if(number % 3 = 0, 'FRANCE', 'GERMANY') FROM numbers(1000);
"

query_id="05056_${CLICKHOUSE_DATABASE}_$RANDOM"

$CLICKHOUSE_CLIENT --query_id="$query_id" -q "
    SELECT count() FROM disj_wire_left AS l, disj_wire_right AS r
    WHERE l.k = r.k AND ((l.name = 'FRANCE' AND r.name = 'GERMANY') OR (l.name = 'GERMANY' AND r.name = 'FRANCE'))
    SETTINGS
        enable_parallel_replicas = 1,
        automatic_parallel_replicas_mode = 0,
        parallel_replicas_plan_based = 1,
        max_parallel_replicas = 3,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
        parallel_replicas_for_non_replicated_merge_tree = 1,
        parallel_replicas_local_plan = 1,
        parallel_replicas_min_number_of_rows_per_replica = 0,
        use_join_disjunctions_push_down = 1
"

# One secondary query per replica, bar the one the initiator runs in process itself.
expected_secondaries=$($CLICKHOUSE_CLIENT -q "
    SELECT count() - 1 FROM system.clusters
    WHERE cluster = 'test_cluster_one_shard_three_replicas_localhost'")

# The secondary queries reach `query_log` a little after the initiator's own row, so one flush can miss
# them - and a replica whose rows are still missing contributes no push-downs to count, which would read
# as a pass for the wrong reason. Wait for all of them, not just the first: if any never arrives,
# `shipped_to_replicas` stays zero and the test fails.
for _ in {1..100}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log, text_log"
    [ "$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.query_log
        WHERE initial_query_id = '$query_id' AND NOT is_initial_query
        SETTINGS enable_parallel_replicas = 0")" -ge "$expected_secondaries" ] && break
    sleep 0.1
done

# The initiator pushes the two partial predicates once while planning; the replicas, which receive that
# plan already optimized, must add none. The other two columns keep a zero from being vacuous:
# `shipped_to_replicas` proves every replica's fragment ran and was accounted for rather than the query
# falling back to a local plan, and `on_initiator` proves the log message this counts still exists.
#
# The secondary queries of the plan-based path are logged with `current_database` = `default` rather than
# the database of the query they belong to, so only the initiator row can be pinned to `currentDatabase()`.
$CLICKHOUSE_CLIENT -q "
    SELECT
        (SELECT count() >= $expected_secondaries FROM system.query_log
         WHERE initial_query_id = '$query_id' AND NOT is_initial_query) AS shipped_to_replicas,
        countIf(query_id = '$query_id') AS on_initiator,
        countIf(query_id != '$query_id') AS on_replicas
    FROM system.text_log
    WHERE message LIKE '%Pushed down partial filter%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE initial_query_id = '$query_id'
            AND (current_database = currentDatabase() OR NOT is_initial_query))
    SETTINGS enable_parallel_replicas = 0
"

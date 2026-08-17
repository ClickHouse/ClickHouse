#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The plan the automatic-parallel-replicas decision builds to cost is usually thrown away, so it must
# not execute the query's subqueries. A `GLOBAL JOIN` against a view materializes the view while the
# plan is built, and that copy used to be a third of every mark the query read.

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_04839;
CREATE TABLE t_04839 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_04839 SELECT number, number FROM numbers(100000);
DROP VIEW IF EXISTS v_04839;
CREATE VIEW v_04839 AS SELECT k, sum(v) AS s FROM t_04839 GROUP BY k;
"

QUERY="SELECT count(), sum(l.k) FROM t_04839 AS l, v_04839 AS r WHERE l.k = r.k AND r.s < 10"

COMMON="enable_analyzer = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 1"

single_node=$($CLICKHOUSE_CLIENT -q "SET $COMMON, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0; $QUERY")
automatic=$($CLICKHOUSE_CLIENT -q "SET $COMMON, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
                                   automatic_parallel_replicas_min_bytes_per_replica = 0; $QUERY")

# The decision must not change the answer.
[ "$single_node" = "$automatic" ] && echo "results match" || echo "results differ: '$single_node' vs '$automatic'"

# ... and must not read more than the single-node plan does, which is what materializing the view in a
# discarded plan would cost.
read_rows() {
    local query_id=$1
    shift
    $CLICKHOUSE_CLIENT --query_id="$query_id" -q "$@" > /dev/null
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    # The `SET` in front of the query is logged under the same query id, so keep only the `SELECT`, and
    # take the most recent one so a re-run does not pick up the previous one.
    $CLICKHOUSE_CLIENT -q "
        SELECT ProfileEvents['SelectedMarks'] FROM system.query_log
        WHERE query_id = '$query_id' AND type = 'QueryFinish' AND is_initial_query
          AND query_kind = 'Select' AND current_database = currentDatabase()
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

marks_single=$(read_rows "04839_single_${CLICKHOUSE_DATABASE}" "SET $COMMON, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0; $QUERY")
marks_auto=$(read_rows "04839_auto_${CLICKHOUSE_DATABASE}" "SET $COMMON, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
                                                            automatic_parallel_replicas_min_bytes_per_replica = 0; $QUERY")

if [ "$marks_auto" -le "$marks_single" ]; then
    echo "no extra marks"
else
    echo "read $marks_auto marks against $marks_single on a single node"
fi

$CLICKHOUSE_CLIENT -q "DROP VIEW v_04839; DROP TABLE t_04839;"

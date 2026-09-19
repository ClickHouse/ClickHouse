#!/usr/bin/env bash
# Tags: zookeeper
# zookeeper: the test creates a ReplicatedMergeTree table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A materialized CTE hidden in a SQL UDF body, in an APPLY transformer or in an APPLY lambda must also keep the
# INSERT off the parallel-replicas insert route, which forwards the resolved query and would ship the initiator's
# temporary table. A UDF with a COLUMNS argument (no CTE) must keep taking the route. UDF names are per database:
# they are server-global and the flaky check runs a test concurrently with itself.
# The CTE bodies are scalar subqueries on purpose: they are folded on the initiator, whereas a materialized CTE
# inside an IN subquery under parallel replicas hits unrelated master bugs on the general path (#112642 and the
# NOT_FOUND_COLUMN_IN_BLOCK issue linked from the PR), which are not what this test covers.

DB="${CLICKHOUSE_DATABASE}"
S="${DB}_s"   # scalar subquery over a materialized CTE, no argument
G="${DB}_g"   # the same, combined with the argument
K="${DB}_k"   # calls G
H="${DB}_h"   # identity, no CTE

ROUTE="--enable_analyzer 1 --enable_materialized_cte 1 --force_materialized_cte 1 --parallel_distributed_insert_select 2 --distributed_foreground_insert 1 --enable_parallel_replicas 1 --parallel_replicas_for_non_replicated_merge_tree 1 --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost --max_parallel_replicas 3 --automatic_parallel_replicas_mode 0 --parallel_replicas_local_plan 1 --parallel_replicas_insert_select_local_pipeline 1 --parallel_replicas_prefer_local_replica 1"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src SELECT number FROM numbers(5);
    CREATE TABLE dst (same UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst_routes_udf', 'r1') ORDER BY tuple();
    CREATE FUNCTION $S AS () -> (WITH c AS MATERIALIZED (SELECT rand64() AS r FROM numbers(1)) SELECT a.r = b.r FROM c AS a, c AS b);
    CREATE FUNCTION $G AS (x) -> toUInt8((WITH c AS MATERIALIZED (SELECT rand64() AS r FROM numbers(1)) SELECT a.r = b.r FROM c AS a, c AS b) AND x = 1);
    CREATE FUNCTION $K AS (x) -> $G(x);
    CREATE FUNCTION $H AS (x) -> x;
"

# Each insert carries a unique alias so its forwarded copies, if any, can be found in system.query_log.
# An error of the insert goes to stderr and fails the test with its message.
run_case() {
    local label="$1" alias="$2" query="$3"
    ${CLICKHOUSE_CLIENT} ${ROUTE} -q "$query"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} -q "SELECT '$label', groupArray(same) FROM dst"
    ${CLICKHOUSE_CLIENT} -q "SELECT '$label forwarded', count() > 0 FROM system.query_log WHERE event_date >= yesterday() AND is_initial_query = 0 AND type = 'QueryFinish' AND query LIKE 'INSERT INTO%SELECT%' AND query LIKE '%$alias%' AND has(databases, currentDatabase())"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE dst"
}

run_case "udf scalar"          "same_udf_scalar_${DB}"   "INSERT INTO dst SELECT $S() AS same_udf_scalar_${DB} FROM src WHERE x = 1"
run_case "udf with argument"   "same_udf_arg_${DB}"      "INSERT INTO dst SELECT $G(x) AS same_udf_arg_${DB} FROM src WHERE x = 1"
run_case "apply udf"           "same_apply_udf_${DB}"    "INSERT INTO dst SELECT * APPLY $G FROM (SELECT x AS same_apply_udf_${DB} FROM src WHERE x = 1)"
run_case "apply lambda"        "same_apply_lambda_${DB}" "INSERT INTO dst SELECT * APPLY (y -> toUInt8((WITH c AS MATERIALIZED (SELECT rand64() AS r FROM numbers(1)) SELECT a.r = b.r FROM c AS a, c AS b) AND y = 1)) FROM (SELECT x AS same_apply_lambda_${DB} FROM src WHERE x = 1)"
run_case "udf calling udf"     "same_udf_udf_${DB}"      "INSERT INTO dst SELECT $K(x) AS same_udf_udf_${DB} FROM src WHERE x = 1"
run_case "udf with COLUMNS argument takes the route" "same_columns_${DB}" "INSERT INTO dst SELECT $H(COLUMNS('^x\$')) AS same_columns_${DB} FROM src WHERE x = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE dst SYNC; DROP TABLE src; DROP FUNCTION $S; DROP FUNCTION $G; DROP FUNCTION $K; DROP FUNCTION $H"

#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-msan
# Regression: a query-plan-cache entry must not be created for a query that calls an executable or a
# WebAssembly UDF. The analyzer resolves those through their own factories, but a plan deserialized
# from the cache rebuilds every `ActionsDAG` function node through `FunctionFactory` alone (see
# `ActionsDAG::deserialize`), so a hit would throw `UNKNOWN_FUNCTION` - or silently call a builtin of
# the same name - instead of running the UDF. A UDF that is deterministic used to pass the
# determinism-based eligibility check, which is exactly the case covered here; the same applies when
# the UDF is called from a row-policy filter rather than from the query itself.
# The plan cache is a single, server-wide cache inspected via `SYSTEM DROP QUERY PLAN CACHE` and
# exact `QueryPlanCacheHits` counts, so the test runs in isolation (see 04489 for the full rationale
# of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

module="wasm_04665_${CLICKHOUSE_DATABASE}"
udf="wasm_udf_04665_${CLICKHOUSE_DATABASE}"
user="user_04665_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --webassembly_udf_max_fuel=1000000"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "
        DROP ROW POLICY IF EXISTS p_04665 ON ${CLICKHOUSE_DATABASE}.t_policy;
        DROP USER IF EXISTS $user;
        DROP FUNCTION IF EXISTS $udf;
        DELETE FROM system.webassembly_modules WHERE name = '$module';
    " > /dev/null 2>&1
}
trap cleanup EXIT

cleanup

$CLICKHOUSE_CLIENT --query "INSERT INTO system.webassembly_modules (name, code)
    SELECT '$module', code FROM input('code String') FORMAT RawBlob" < "${CUR_DIR}/wasm/identity_int.wasm"

$CLICKHOUSE_CLIENT --query "
    CREATE FUNCTION $udf
        LANGUAGE WASM FROM '$module' :: 'identity_msgpack_i32'
        ARGUMENTS (x Int32) RETURNS Int32
        ABI BUFFERED_V1
        DETERMINISTIC;

    DROP TABLE IF EXISTS t_wasm;
    DROP TABLE IF EXISTS t_policy;
    CREATE TABLE t_wasm (x Int32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_policy (x Int32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_wasm VALUES (1), (2), (3);
    INSERT INTO t_policy VALUES (1), (2), (3);

    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_policy TO $user;
"

run()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1" 2>&1
}

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$1" 2>&1
}

# Number of plan cache hits recorded for the most recent run of a query matching $1.
hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query LIKE '$1%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

echo "-- 1. a deterministic WASM UDF in the query: NOT cacheable"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
UDF_QUERY="SELECT sum($udf(x)) FROM ${CLICKHOUSE_DATABASE}.t_wasm"
echo "-- first run: $(run "$UDF_QUERY")"
echo "-- second run (same result, the UDF really runs): $(run "$UDF_QUERY")"
echo "-- hits after a repeat (must be 0, not cached): $(hits_of_last_run "SELECT sum($udf")"

echo "-- 2. the same query without the UDF: cacheable"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
PLAIN_QUERY="SELECT sum(x) FROM ${CLICKHOUSE_DATABASE}.t_wasm"
run "$PLAIN_QUERY" > /dev/null
run "$PLAIN_QUERY" > /dev/null
echo "-- hits after a repeat (must be 1, cached): $(hits_of_last_run 'SELECT sum(x) FROM')"

echo "-- 3. a deterministic executable UDF in the query: NOT cacheable"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
EXEC_QUERY="SELECT sum(test_function_deterministic(toUInt64(x))) FROM ${CLICKHOUSE_DATABASE}.t_wasm"
echo "-- first run: $(run "$EXEC_QUERY")"
echo "-- second run (same result, the UDF really runs): $(run "$EXEC_QUERY")"
echo "-- hits after a repeat (must be 0, not cached): $(hits_of_last_run 'SELECT sum(test_function_deterministic')"

echo "-- 4. a WASM UDF in a row-policy filter: NOT cacheable"
$CLICKHOUSE_CLIENT --query "
    CREATE ROW POLICY p_04665 ON ${CLICKHOUSE_DATABASE}.t_policy USING $udf(x) <= 2 TO $user;
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
POLICY_QUERY="SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_policy"
echo "-- first run (rows x <= 2): $(run_user "$POLICY_QUERY")"
echo "-- second run: $(run_user "$POLICY_QUERY")"
echo "-- hits after a repeat (must be 0, not cached): $(hits_of_last_run 'SELECT count() FROM')"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_wasm;
    DROP TABLE t_policy;
"

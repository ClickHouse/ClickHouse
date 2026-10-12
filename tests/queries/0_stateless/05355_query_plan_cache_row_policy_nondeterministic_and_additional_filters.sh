#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression: the plan cache must not store plans whose filters are invisible to its eligibility checks
# and dependency tracking:
#   - a row policy calling a non-deterministic function (e.g. `today`) is planned into the cached
#     filter step, so a hit could keep enforcing the analysis-time value;
#   - `additional_table_filters` / `additional_result_filter` are injected during analysis, so a
#     scalar subquery inside them would be folded into the plan without its table becoming a
#     dependency.
# The plan cache is a single, server-wide cache inspected via `SYSTEM DROP QUERY PLAN CACHE` and
# exact `QueryPlanCacheHits` counts, and the test creates a global user, so it runs in isolation
# (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05355_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_05355;
    DROP TABLE IF EXISTS lim_05355;
    CREATE TABLE t_05355 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE lim_05355 (v UInt64) ENGINE = MergeTree ORDER BY v;
    INSERT INTO t_05355 VALUES (1), (2), (3), (4), (5);
    INSERT INTO lim_05355 VALUES (2);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05355 TO $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.lim_05355 TO $user;
"

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

echo "-- 1. deterministic row policy: query IS cacheable"
$CLICKHOUSE_CLIENT --query "CREATE ROW POLICY p_05355 ON ${CLICKHOUSE_DATABASE}.t_05355 USING x <= 3 TO $user"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
Q1="SELECT /* q1 */ sum(x) FROM ${CLICKHOUSE_DATABASE}.t_05355"
echo "-- result: $(run_user "$Q1")"
run_user "$Q1" > /dev/null
echo "-- hits after a repeat (must be 1): $(hits_of_last_run 'SELECT /* q1 */')"

echo "-- 2. row policy calling a non-deterministic function: query is NOT cacheable"
$CLICKHOUSE_CLIENT --query "ALTER ROW POLICY p_05355 ON ${CLICKHOUSE_DATABASE}.t_05355 USING x <= 3 AND today() > '2000-01-01'"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
Q2="SELECT /* q2 */ sum(x) FROM ${CLICKHOUSE_DATABASE}.t_05355"
echo "-- result: $(run_user "$Q2")"
run_user "$Q2" > /dev/null
echo "-- hits after a repeat (must be 0): $(hits_of_last_run 'SELECT /* q2 */')"
$CLICKHOUSE_CLIENT --query "DROP ROW POLICY p_05355 ON ${CLICKHOUSE_DATABASE}.t_05355"

echo "-- 3. additional_table_filters with a scalar subquery: query is NOT cacheable"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
Q3="SELECT /* q3 */ sum(x) FROM ${CLICKHOUSE_DATABASE}.t_05355 SETTINGS additional_table_filters = {'${CLICKHOUSE_DATABASE}.t_05355': 'x <= (SELECT max(v) FROM ${CLICKHOUSE_DATABASE}.lim_05355)'}"
echo "-- result: $(run_user "$Q3")"
run_user "$Q3" > /dev/null
echo "-- hits after a repeat (must be 0): $(hits_of_last_run 'SELECT /* q3 */')"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.lim_05355 VALUES (4)"
echo "-- boundary moved to 4, reflected immediately: $(run_user "$Q3")"

echo "-- 4. additional_result_filter: query is NOT cacheable"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
Q4="SELECT /* q4 */ x FROM ${CLICKHOUSE_DATABASE}.t_05355 ORDER BY x SETTINGS additional_result_filter = 'x >= 4'"
echo "-- result: $(run_user "$Q4" | tr '\n' ' ')"
run_user "$Q4" > /dev/null
echo "-- hits after a repeat (must be 0): $(hits_of_last_run 'SELECT /* q4 */')"

$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY IF EXISTS p_05355 ON ${CLICKHOUSE_DATABASE}.t_05355;
    DROP USER IF EXISTS $user;
    DROP TABLE t_05355;
    DROP TABLE lim_05355;
"

#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a read through an `Alias` table must not be cached. The plan carries semantics of
# the alias's *target* that the dependency record - which describes the alias alone - cannot
# fingerprint or re-check on a hit:
#   - `PlannerJoinTree::getEffectiveRowPolicyFilter` bakes the combination of the alias's and the
#     target's `SELECT` row policies into the plan, so tightening the target's policy would leave a
#     cached entry valid while it keeps returning rows the current policy hides;
#   - `StorageAlias::read` re-checks the plan's column names against the target table, so a hit
#     replays the columns chosen at store time even after the target grants changed.
# The plan cache is a single, server-wide cache inspected via SYSTEM DROP QUERY PLAN CACHE, and the
# test creates a global user and row policies, so it runs in isolation (see 04489 for the full
# rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05141_${CLICKHOUSE_DATABASE}"
policy="policy_05141_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS alias_target;
    DROP TABLE IF EXISTS alias_tbl;
    CREATE TABLE alias_target (a UInt8, b UInt64) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE alias_tbl ENGINE = Alias('${CLICKHOUSE_DATABASE}', 'alias_target');
    INSERT INTO alias_target VALUES (1, 10), (2, 20), (3, 30);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.alias_target TO $user;
    GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.alias_tbl TO $user;
"

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$1" 2>&1
}

hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND current_database = currentDatabase()
          AND user = '$user'
          AND query LIKE '%alias_tbl%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

QUERY="SELECT sum(b) FROM ${CLICKHOUSE_DATABASE}.alias_tbl"

echo "-- 1. an ordinary read through an Alias is never cached"
run_user "$QUERY"
echo "-- hits: $(hits_of_last_run)"
run_user "$QUERY"
echo "-- hits: $(hits_of_last_run)"

echo "-- 2. a row policy tightened on the target table takes effect immediately"
$CLICKHOUSE_CLIENT --query "CREATE ROW POLICY $policy ON ${CLICKHOUSE_DATABASE}.alias_target USING a <= 2 TO $user"
run_user "$QUERY"
echo "-- hits: $(hits_of_last_run)"
$CLICKHOUSE_CLIENT --query "ALTER ROW POLICY $policy ON ${CLICKHOUSE_DATABASE}.alias_target USING a <= 1"
run_user "$QUERY"
echo "-- hits: $(hits_of_last_run)"
$CLICKHOUSE_CLIENT --query "DROP ROW POLICY $policy ON ${CLICKHOUSE_DATABASE}.alias_target"

echo "-- 3. a zero-column read through an Alias is not cached either, so revoking the column"
echo "-- chosen at planning time still re-plans with the other one"
COUNT_QUERY="SELECT count() FROM ${CLICKHOUSE_DATABASE}.alias_tbl"
run_user "$COUNT_QUERY"
echo "-- hits: $(hits_of_last_run)"
$CLICKHOUSE_CLIENT --query "
    REVOKE SELECT(a) ON ${CLICKHOUSE_DATABASE}.alias_target FROM $user;
    REVOKE SELECT(a) ON ${CLICKHOUSE_DATABASE}.alias_tbl FROM $user;
"
run_user "$COUNT_QUERY"
echo "-- hits: $(hits_of_last_run)"

echo "-- 4. an Alias folded into a scalar subquery is not cached, so losing the grant on the"
echo "-- target's column denies the query instead of serving the baked constant"
SCALAR_QUERY="SELECT (SELECT sum(b) FROM ${CLICKHOUSE_DATABASE}.alias_tbl) SETTINGS query_plan_cache_allow_scalar_subqueries = 1"
run_user "$SCALAR_QUERY"
run_user "$SCALAR_QUERY"
echo "-- hits: $(hits_of_last_run)"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT(b) ON ${CLICKHOUSE_DATABASE}.alias_target FROM $user"
run_user "$SCALAR_QUERY" | grep -Fo "ACCESS_DENIED" | uniq

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user;
    DROP TABLE alias_tbl;
    DROP TABLE alias_target;
"

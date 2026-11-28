#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a query-plan-cache hit over an `SQL SECURITY INVOKER` view must not re-check the
# view's access more weakly than a miss, and must not be stricter than a miss either. A view is
# expanded at plan time and has no `ReadFromTable` leaf, so its exact selected columns cannot be
# recovered for the recheck; a hit therefore requires table-level SELECT on the view. Otherwise a
# column-level "any column" recheck could pass after the selected column's grant was revoked while
# another column stayed granted, and the hit would still return the revoked column. A miss checks
# the precise per-column grant on the view during analysis
# (`prepareBuildQueryPlanForTableExpression`), so a session holding only column grants on the view
# must not get an entry stored at all: it would pass the miss and then be denied on the hit. The
# underlying table is granted at the table level, so only the view's access is exercised here.
# The plan cache is a single, server-wide cache inspected via SYSTEM DROP QUERY PLAN CACHE, and the
# test creates a global user, so it runs in isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_04494_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP VIEW IF EXISTS v_inv;
    DROP TABLE IF EXISTS t_base;
    CREATE TABLE t_base (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_base VALUES (1, 10), (2, 20);
    CREATE VIEW v_inv AS SELECT a, b FROM t_base;

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
"

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query_id="04494_${CLICKHOUSE_DATABASE}_$2" --query "$1" 2>&1
}

hits()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND event_date >= yesterday()
          AND type = 'QueryFinish'
          AND query_id = '04494_${CLICKHOUSE_DATABASE}_$1'
          AND user = '$user'"
}

validation_misses()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheValidationMisses']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND event_date >= yesterday()
          AND type = 'QueryFinish'
          AND query_id = '04494_${CLICKHOUSE_DATABASE}_$1'
          AND user = '$user'"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

# Table-level SELECT on the base table so its plan-leaf recheck always passes; only the view matters.
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_base TO $user"
# Column-level grants on the view only: the query is allowed (analysis checks the selected view
# column), but it is not cached, because a hit would re-check the view at the table level.
$CLICKHOUSE_CLIENT --query "GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.v_inv TO $user"

VIEW_QUERY="SELECT a FROM ${CLICKHOUSE_DATABASE}.v_inv ORDER BY a"

echo "-- first run (allowed, per-column grant on the view):"
run_user "$VIEW_QUERY" col1
echo "-- second run with only column grants on the view is allowed and is not a hit:"
run_user "$VIEW_QUERY" col2
hits col2
echo "-- revoking the selected column while another remains denies the query:"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT(a) ON ${CLICKHOUSE_DATABASE}.v_inv FROM $user"
run_user "$VIEW_QUERY" revoked | grep -Fo "ACCESS_DENIED" | uniq
echo "-- after granting table-level SELECT on the view the plan is cached and hit:"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_inv TO $user"
run_user "$VIEW_QUERY" table1
run_user "$VIEW_QUERY" table2
hits table2
echo "-- replacing table-level SELECT with a column grant: the cached entry is not hit, the query is allowed:"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.v_inv FROM $user"
$CLICKHOUSE_CLIENT --query "GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.v_inv TO $user"
run_user "$VIEW_QUERY" replanned
hits replanned
echo "-- the refused hit is counted as a validation miss although the schema and row policies are unchanged:"
validation_misses replanned

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user;
    DROP VIEW v_inv;
    DROP TABLE t_base;
"

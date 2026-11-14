#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a materialized view is NOT expanded into a cacheable logical plan - `PlannerJoinTree`
# inlines only `StorageView`, so a `StorageMaterializedView` stays a `ReadFromTable` leaf with an exact
# output column set and is executed by `StorageMaterializedView::readImpl` against its target table.
# The dependency walk must therefore keep the leaf's precise columns for it
# (`isViewExpandedInCacheablePlan`) instead of marking it `columns_unknown` as it does for expanded
# views: a synthetic `columns_unknown` would upgrade the hit recheck to table-level `SELECT`, which
# column grants do not satisfy, so a user holding only `SELECT(a)` on the view would succeed on the
# miss and get `ACCESS_DENIED` on the hit. Reading the view still checks `SELECT` on the source table
# of its defining query, but that check lives in `readImpl` and therefore runs on the hit as well.
# The complementary expanded-view behaviour (table-level recheck required) is covered by 04494.
# The plan cache is a single, server-wide cache inspected via SYSTEM DROP QUERY PLAN CACHE and exact
# metric values, and the test creates a global user, so it runs in isolation (see 04489 for the full
# rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_04651_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS mv_mat;
    DROP TABLE IF EXISTS t_target;
    DROP TABLE IF EXISTS t_src;
    CREATE TABLE t_src (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE t_target (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    -- No explicit \`SQL SECURITY\` clause: a materialized view cannot be \`INVOKER\`, and a DEFINER or
    -- NONE one executes under an overridden security context that a cached plan cannot replay, so
    -- \`isStorageEligibleForPlanCache\` refuses to cache it. With the default
    -- \`ignore_empty_sql_security_in_create_view_query = 1\` the view gets no security type at all,
    -- which leaves it eligible - exactly the case this test exercises.
    CREATE MATERIALIZED VIEW mv_mat TO t_target AS SELECT a, b FROM t_src;
    INSERT INTO t_src VALUES (1, 10), (2, 20);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
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
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query LIKE 'SELECT a FROM%mv_mat%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

# Only column-level grants: reading the materialized view goes through its own plan leaf, so both the
# miss and the hit must be satisfied by the per-column grants. `readImpl` also checks the selected
# columns on the source table of the view's defining query, on the miss and on the hit alike.
$CLICKHOUSE_CLIENT --query "GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.mv_mat TO $user"
$CLICKHOUSE_CLIENT --query "GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.t_src TO $user"

MV_QUERY="SELECT a FROM ${CLICKHOUSE_DATABASE}.mv_mat ORDER BY a"

echo "-- miss (allowed, per-column grants):"
run_user "$MV_QUERY"
echo "-- hits: $(hits_of_last_run)"
echo "-- hit with the same column grants must stay allowed:"
run_user "$MV_QUERY"
echo "-- hits: $(hits_of_last_run)"
echo "-- revoking the selected column on the view denies the hit:"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT(a) ON ${CLICKHOUSE_DATABASE}.mv_mat FROM $user"
run_user "$MV_QUERY" | grep -Fo "ACCESS_DENIED" | uniq
echo "-- a denied hit evicts the entry, so the first run after re-granting is a miss that re-stores it:"
$CLICKHOUSE_CLIENT --query "GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.mv_mat TO $user"
run_user "$MV_QUERY"
echo "-- hits: $(hits_of_last_run)"
echo "-- the source table of the view's defining query is not a plan dependency, so a schema change"
echo "-- there does not invalidate the entry (the view's own leaf is re-resolved on every hit):"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_src ADD COLUMN c UInt64"
run_user "$MV_QUERY"
echo "-- hits: $(hits_of_last_run)"

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user;
    DROP TABLE mv_mat;
    DROP TABLE t_target;
    DROP TABLE t_src;
"

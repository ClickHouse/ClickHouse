#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a query-plan-cache hit must re-check SELECT access for every column the cached
# plan reads. Two access holes are covered:
#   1. A self-join reads a table through two plan leaves; the dependency must union both column
#      sets, so revoking SELECT on a column read by either leaf denies the next hit (a plain dedup
#      that kept one leaf's columns could let the hit pass after the dropped column was revoked).
#   2. A table reached only through a scalar subquery is folded into a constant during analysis and
#      has no plan leaf, so its exact columns are unknown and a hit must require table-level SELECT
#      (a column-level "any column" recheck could pass while the baked plan still reads a revoked
#      column).
# The plan cache is a single, server-wide cache inspected via SYSTEM DROP QUERY PLAN CACHE, and the
# test creates a global user, so it runs in isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_04492_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query_plan_cache_allow_scalar_subqueries=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_self;
    DROP TABLE IF EXISTS t_scalar;
    CREATE TABLE t_self (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE t_scalar (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_self VALUES (1, 1), (2, 2);
    INSERT INTO t_scalar VALUES (1, 100), (2, 200);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
"

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$1" 2>&1
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

echo "-- 1. self-join: column requirements are unioned across both plan leaves"
$CLICKHOUSE_CLIENT --query "GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.t_self TO $user"
# Reads a through t1 and b through t2 (the join key), so the union of the two leaves is {a, b}.
SELF_QUERY="SELECT t1.a FROM ${CLICKHOUSE_DATABASE}.t_self AS t1 JOIN ${CLICKHOUSE_DATABASE}.t_self AS t2 ON t1.a = t2.b ORDER BY t1.a"
echo "-- miss (allowed):"
run_user "$SELF_QUERY"
echo "-- hit (both columns still granted, allowed):"
run_user "$SELF_QUERY"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT(b) ON ${CLICKHOUSE_DATABASE}.t_self FROM $user"
echo "-- hit after revoking SELECT(b) must be denied:"
run_user "$SELF_QUERY" | grep -Fo "ACCESS_DENIED" | uniq

echo "-- 2. scalar subquery: a hit requires table-level SELECT"
# Only column-level grants: the miss succeeds (analysis checks columns), but a hit re-checks the
# folded scalar-subquery table at the table level, which column grants do not satisfy.
$CLICKHOUSE_CLIENT --query "GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.t_scalar TO $user"
SCALAR_QUERY="SELECT a, (SELECT sum(b) FROM ${CLICKHOUSE_DATABASE}.t_scalar) AS s FROM ${CLICKHOUSE_DATABASE}.t_scalar ORDER BY a"
echo "-- miss (allowed):"
run_user "$SCALAR_QUERY"
echo "-- hit with only column grants must be denied (table-level SELECT required):"
run_user "$SCALAR_QUERY" | grep -Fo "ACCESS_DENIED" | uniq
echo "-- after granting table-level SELECT the hit is allowed:"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_scalar TO $user"
run_user "$SCALAR_QUERY"

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user;
    DROP TABLE t_self;
    DROP TABLE t_scalar;
"

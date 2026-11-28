#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-ordinary-database, no-replicated-database
# A scalar subquery is folded into a constant during analysis, so a plan cache hit never executes
# the read it came from. The access and identity revalidation of a hit must still hold for it:
#   1. `MaterializedView` and `Buffer` check `SELECT` on another table (the defining source, the
#      destination) inside their own read. That check is not part of the dependency record, so a
#      scalar over them must not be cached: after revoking the grant on the other table, the query
#      must throw `ACCESS_DENIED` exactly like a fresh execution.
#   2. A scalar `count` can be answered by the trivial count shortcut, which reads no data. The table
#      must still be recorded as an analyzed storage, otherwise it is dropped from the dependencies,
#      and neither a revoke nor a `DROP`/`CREATE` of the table invalidates the folded value.
# The plan cache is a single, server-wide cache and the test creates a global user, so it runs in
# isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05394_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query_plan_cache_allow_scalar_subqueries=1 --optimize_trivial_count_query=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS mv;
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS buf;
    DROP TABLE IF EXISTS dst;
    DROP TABLE IF EXISTS t_count;
    CREATE TABLE src (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY a AS SELECT a FROM src;
    INSERT INTO src VALUES (1), (2);
    CREATE TABLE dst (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE buf (a UInt64) ENGINE = Buffer('${CLICKHOUSE_DATABASE}', 'dst', 1, 1000, 1000, 1000000000, 1000000000, 1000000000, 1000000000);
    INSERT INTO dst VALUES (10), (20);
    CREATE TABLE t_count (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_count VALUES (1), (2), (3);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.mv TO $user;
    GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.src TO $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.buf TO $user;
    GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.dst TO $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_count TO $user;
    SYSTEM DROP QUERY PLAN CACHE;
"

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$1" 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|^[0-9]+$'
}

run()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1"
}

echo "-- 1. MaterializedView in a scalar subquery"
run_user "SELECT (SELECT sum(a) FROM mv)"
run_user "SELECT (SELECT sum(a) FROM mv)"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT(a) ON ${CLICKHOUSE_DATABASE}.src FROM $user"
run_user "SELECT (SELECT sum(a) FROM mv)"

echo "-- 1. Buffer in a scalar subquery"
run_user "SELECT (SELECT sum(a) FROM buf)"
run_user "SELECT (SELECT sum(a) FROM buf)"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT(a) ON ${CLICKHOUSE_DATABASE}.dst FROM $user"
run_user "SELECT (SELECT sum(a) FROM buf)"

echo "-- 2. trivial count in a scalar subquery, revoke"
run_user "SELECT (SELECT count() FROM t_count)"
run_user "SELECT (SELECT count() FROM t_count)"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.t_count FROM $user"
run_user "SELECT (SELECT count() FROM t_count)"

echo "-- 2. trivial count in a scalar subquery, table replaced"
run "SELECT (SELECT count() FROM t_count)"
run "SELECT (SELECT count() FROM t_count)"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_count SYNC;
    CREATE TABLE t_count (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_count VALUES (1);
"
run "SELECT (SELECT count() FROM t_count)"

$CLICKHOUSE_CLIENT --query "
    DROP USER $user;
    DROP TABLE mv;
    DROP TABLE src;
    DROP TABLE buf;
    DROP TABLE dst;
    DROP TABLE t_count;
"

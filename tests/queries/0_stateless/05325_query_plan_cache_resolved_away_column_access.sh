#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a query-plan-cache hit must re-check `SELECT` access for the columns that the
# planner resolves away before the main access check - `indexHint` arguments and `ALIAS` columns
# used only in `PREWHERE` (see `checkAccessRightsForColumnsResolvedAway`). A miss requires the grant
# on them, so a hit after revoking it must be denied too. The same check also applies to a
# parameterized view, which is planned as a table function node.
# The plan cache is a single, server-wide cache and the test creates global users, so it runs in
# isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05325_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_resolved_away;
    CREATE TABLE t_resolved_away (a UInt64, c UInt64, b UInt64 ALIAS c + 1) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_resolved_away VALUES (1, 1), (2, 2);
    DROP VIEW IF EXISTS pv_resolved_away;
    CREATE VIEW pv_resolved_away AS SELECT a, c FROM t_resolved_away WHERE a > {p:UInt64};

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT(a, b, c) ON ${CLICKHOUSE_DATABASE}.t_resolved_away TO $user;
"

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$1" 2>&1 | grep -Eo '^[0-9]+$|ACCESS_DENIED' | uniq
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

QUERIES=(
    "SELECT a FROM ${CLICKHOUSE_DATABASE}.t_resolved_away WHERE indexHint(c = 1) AND a = 1"
    "SELECT a FROM ${CLICKHOUSE_DATABASE}.t_resolved_away PREWHERE b > 0 WHERE a = 1"
    "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_resolved_away WHERE indexHint(c = 1)"
)

for query in "${QUERIES[@]}"
do
    echo "-- ${query//${CLICKHOUSE_DATABASE}./}"
    echo "miss:"
    run_user "$query"
    echo "hit:"
    run_user "$query"
done

$CLICKHOUSE_CLIENT --query "REVOKE SELECT(b, c) ON ${CLICKHOUSE_DATABASE}.t_resolved_away FROM $user"

for query in "${QUERIES[@]}"
do
    echo "-- after revoke: ${query//${CLICKHOUSE_DATABASE}./}"
    echo "without the cache:"
    $CLICKHOUSE_CLIENT --user="$user" --query "$query" 2>&1 | grep -Eo '^[0-9]+$|ACCESS_DENIED' | uniq
    echo "hit:"
    run_user "$query"
done

echo "-- parameterized view: an indexHint argument requires a grant on the view column"
$CLICKHOUSE_CLIENT --query "
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_resolved_away TO $user;
    GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.pv_resolved_away TO $user;
"
PV_QUERY="SELECT a FROM ${CLICKHOUSE_DATABASE}.pv_resolved_away(p = 0) WHERE indexHint(c = 1) AND a = 1"
$CLICKHOUSE_CLIENT --user="$user" --query "$PV_QUERY" 2>&1 | grep -Eo '^[0-9]+$|ACCESS_DENIED' | uniq
$CLICKHOUSE_CLIENT --query "GRANT SELECT(c) ON ${CLICKHOUSE_DATABASE}.pv_resolved_away TO $user"
$CLICKHOUSE_CLIENT --user="$user" --query "$PV_QUERY" 2>&1 | grep -Eo '^[0-9]+$|ACCESS_DENIED' | uniq

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user;
    DROP VIEW pv_resolved_away;
    DROP TABLE t_resolved_away;
"

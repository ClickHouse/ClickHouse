#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a query-plan-cache hit must re-check `SELECT` access for the columns the query
# *selects*, not for the physical columns the plan reads. They differ for `ALIAS` columns: for
# `b UInt64 ALIAS a + 1`, `SELECT b FROM t` requires `SELECT(b)` while reading `a`.
# Recording the plan leaf's output header therefore both let a hit serve `b` after `REVOKE
# SELECT(b)` (as long as `SELECT(a)` remained) and denied a user who only held `SELECT(b)`.
# The plan cache is a single, server-wide cache and the test creates global users, so it runs in
# isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

alias_user="alias_user_05024_${CLICKHOUSE_DATABASE}"
physical_user="physical_user_05024_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_alias;
    CREATE TABLE t_alias (a UInt64, b UInt64 ALIAS a + 1) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_alias VALUES (1), (2);

    DROP USER IF EXISTS $alias_user, $physical_user;
    CREATE USER $alias_user, $physical_user;
    REVOKE ALL ON *.* FROM $alias_user, $physical_user;
    GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.t_alias TO $alias_user;
    -- Holds the grant on the alias only, never on the column it is computed from.
    GRANT SELECT(b) ON ${CLICKHOUSE_DATABASE}.t_alias TO $physical_user;
"

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$1" $SETTINGS --query "$2" 2>&1
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

QUERY="SELECT b FROM ${CLICKHOUSE_DATABASE}.t_alias ORDER BY b"

echo "-- 1. revoking the grant on the alias column denies the next hit"
echo "-- miss (allowed):"
run_user "$alias_user" "$QUERY"
echo "-- hit (allowed):"
run_user "$alias_user" "$QUERY"
# SELECT(a) stays granted: a recheck of the plan leaf's physical columns would still pass.
$CLICKHOUSE_CLIENT --query "REVOKE SELECT(b) ON ${CLICKHOUSE_DATABASE}.t_alias FROM $alias_user"
echo "-- the same query without the cache is denied:"
$CLICKHOUSE_CLIENT --user="$alias_user" --query "$QUERY" 2>&1 | grep -Fo "ACCESS_DENIED" | uniq
echo "-- hit after revoking SELECT(b) must be denied too:"
run_user "$alias_user" "$QUERY" | grep -Fo "ACCESS_DENIED" | uniq

echo "-- 2. a grant on the alias column alone is enough for a hit"
echo "-- miss (allowed):"
run_user "$physical_user" "$QUERY"
echo "-- hit (allowed, SELECT(a) is not required):"
run_user "$physical_user" "$QUERY"

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $alias_user, $physical_user;
    DROP TABLE t_alias;
"

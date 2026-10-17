#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a bare table name on the right side of `IN` / `GLOBAL IN` is a dependency of the
# cached plan. The parser emits a plain identifier there rather than a table expression, and a view
# named there is expanded into the set sub-plan, whose leaves are only the view's body tables:
#   1. Replacing the view must invalidate the cached plan, so a hit cannot keep the old view body.
#   2. Revoking `SELECT` on the view must deny the next execution, as it does on a miss.
#   3. A `Set` storage named there is recorded as well, and its current contents are used.
# The plan cache is a single, server-wide cache inspected via SYSTEM DROP QUERY PLAN CACHE, and the
# test creates a global user, so it runs in isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05366_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP VIEW IF EXISTS v_in;
    DROP TABLE IF EXISTS t_in;
    DROP TABLE IF EXISTS t_set;
    CREATE TABLE t_in (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_in SELECT number FROM numbers(10);
    CREATE VIEW v_in AS SELECT id FROM t_in WHERE id < 3;
    CREATE TABLE t_set (id UInt64) ENGINE = Set;
    INSERT INTO t_set VALUES (1), (2);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
"

run()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1"
}

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$1" 2>&1
}

hits_of()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query = '$1'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

echo "-- 1. IN view: miss, hit, then replacing the view invalidates the entry"
Q_IN="SELECT count() FROM t_in WHERE id IN v_in"
run "$Q_IN"
echo "-- hits: $(hits_of "$Q_IN")"
run "$Q_IN"
echo "-- hits: $(hits_of "$Q_IN")"
$CLICKHOUSE_CLIENT --query "CREATE OR REPLACE VIEW v_in AS SELECT id FROM t_in WHERE id < 5"
run "$Q_IN"
echo "-- hits: $(hits_of "$Q_IN")"

echo "-- 1b. GLOBAL IN view"
Q_GLOBAL_IN="SELECT count() FROM t_in WHERE id GLOBAL IN v_in"
run "$Q_GLOBAL_IN"
run "$Q_GLOBAL_IN"
echo "-- hits: $(hits_of "$Q_GLOBAL_IN")"
$CLICKHOUSE_CLIENT --query "CREATE OR REPLACE VIEW v_in AS SELECT id FROM t_in WHERE id < 7"
run "$Q_GLOBAL_IN"
echo "-- hits: $(hits_of "$Q_GLOBAL_IN")"

echo "-- 2. revoking SELECT on the view denies the next execution"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_in TO $user"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_in TO $user"
Q_USER="SELECT count() FROM t_in WHERE id NOT IN v_in"
run_user "$Q_USER"
run_user "$Q_USER"
echo "-- hits: $(hits_of "$Q_USER")"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.v_in FROM $user"
run_user "$Q_USER" | grep -Fo "ACCESS_DENIED" | uniq

echo "-- 3. IN Set storage: the current contents of the set are used"
Q_SET="SELECT count() FROM t_in WHERE id IN t_set"
run "$Q_SET"
run "$Q_SET"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_set VALUES (3)"
run "$Q_SET"

$CLICKHOUSE_CLIENT --query "
    DROP USER $user;
    DROP VIEW v_in;
    DROP TABLE t_in;
    DROP TABLE t_set;
"

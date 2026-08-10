#!/usr/bin/env bash

# Regression test for `query_cache_use_only_when_data_was_not_changed` through a `SQL SECURITY DEFINER`
# / `NONE` view (issue #108713, PR #108721). Reading such a view executes its stored `SELECT` under
# `getSQLSecurityOverriddenContext`, so the rows it returns are the rows the *effective* reader sees.
# The modification hash must be sampled under that same context: a non-trivial `SELECT` row policy of
# the definer changes what the view returns while every table's data stays the same, so the consistency
# check has to fail closed on it. Sampled under the caller context instead, it looked at the invoker's
# policies (which the read never applies) and missed the definer's, so a cached result survived an
# `ALTER ROW POLICY` for the definer and a stale row was served - the last block below shows exactly
# that transition.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# Users and row policies are server-wide, so make every name unique per run: concurrent runs (e.g. the
# flaky check) must not see each other's grants or policies. The database name folded into the queries
# below keeps the stored query texts - and so the `system.query_cache` filtering - unique per run too.
definer="definer_04841_${CLICKHOUSE_DATABASE}"
open_definer="open_definer_04841_${CLICKHOUSE_DATABASE}"

# Pin every query-cache setting so the flaky check's settings randomizer cannot change the outcome.
qc="use_query_cache = 1, enable_reads_from_query_cache = 1, enable_writes_to_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1"

# The tables read by a definer with a row policy are separate from the ones read without: a table that
# has any row policy is not readable at all by a user none of its policies mention, so a single shared
# table could not serve both cases.
$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer}, ${open_definer};
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${definer}, ${open_definer};

    CREATE TABLE t_policy_04841 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_open_04841 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE target_policy_04841 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE target_open_04841 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_policy_04841 VALUES (1), (2), (3);
    INSERT INTO t_open_04841 VALUES (1), (2), (3);
    INSERT INTO target_policy_04841 VALUES (1), (2), (3);
    INSERT INTO target_open_04841 VALUES (1), (2), (3);

    CREATE ROW POLICY policy_04841 ON ${CLICKHOUSE_DATABASE}.t_policy_04841 FOR SELECT USING x = 1 TO ${definer};
    CREATE ROW POLICY policy_target_04841 ON ${CLICKHOUSE_DATABASE}.target_policy_04841 FOR SELECT USING x = 1 TO ${definer};

    -- The definer of this view sees only one row of the table, so what the view returns depends on a row
    -- policy that the invoker's context does not have.
    CREATE VIEW v_definer_04841 DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT sum(x) AS s FROM t_policy_04841;
    -- No row policy anywhere: the consistency check works as usual.
    CREATE VIEW v_open_04841 DEFINER = ${open_definer} SQL SECURITY DEFINER AS SELECT sum(x) AS s FROM t_open_04841;
    CREATE VIEW v_none_04841 SQL SECURITY NONE AS SELECT sum(x) AS s FROM t_open_04841;

    -- Reading a materialized view reads its target table under the same effective context. It applies no
    -- row policy of the target, so no policy change can move its result; the check is only expected to
    -- follow the effective reader's grants, and it fails closed on that reader's policy conservatively.
    CREATE MATERIALIZED VIEW mv_definer_04841 TO target_policy_04841 (x UInt64)
        DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM t_open_04841;
    CREATE MATERIALIZED VIEW mv_open_04841 TO target_open_04841 (x UInt64)
        DEFINER = ${open_definer} SQL SECURITY DEFINER AS SELECT x FROM t_open_04841;
"

# Two runs of each view. The views whose effective reader has a row policy must fail closed - nothing is
# stored, so neither run can be a cache hit. The others are stored on the first run and hit on the second.
for view in v_definer_04841 v_open_04841 v_none_04841
do
    $CLICKHOUSE_CLIENT -q "SELECT '${view}', s FROM ${view} WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"
    $CLICKHOUSE_CLIENT -q "SELECT '${view}', s FROM ${view} WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"
done
for view in mv_definer_04841 mv_open_04841
do
    $CLICKHOUSE_CLIENT -q "SELECT '${view}', sum(x) FROM ${view} WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"
    $CLICKHOUSE_CLIENT -q "SELECT '${view}', sum(x) FROM ${view} WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"
done

# Cache hits of the two runs of each view.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
for view in v_definer_04841 v_open_04841 v_none_04841 mv_definer_04841 mv_open_04841
do
    $CLICKHOUSE_CLIENT -q "
    SELECT '${view} hits', groupArray(hits)
    FROM
    (
        SELECT ProfileEvents['QueryCacheHits'] AS hits
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
          AND current_database = currentDatabase()
          AND query LIKE 'SELECT ''${view}''%'
        ORDER BY event_time_microseconds
    )"
done

# Nothing may have been stored for the views whose effective reader has a row policy (the query texts
# embed the database name, so entries from concurrent runs of this test do not match the filter).
$CLICKHOUSE_CLIENT -q "
    SELECT 'row-policy definer stored entries', count()
    FROM system.query_cache
    WHERE query LIKE '%${CLICKHOUSE_DATABASE}%'
      AND (query LIKE '%v_definer_04841%' OR query LIKE '%mv_definer_04841%')"

# The stale result the fail-close prevents: altering the definer's policy changes what the view returns
# while the table's data is untouched. The second run must report the new value, not the cached one.
echo 'policy alter changes the result:'
$CLICKHOUSE_CLIENT -q "SELECT s FROM v_definer_04841 SETTINGS ${qc}"
$CLICKHOUSE_CLIENT -q "ALTER ROW POLICY policy_04841 ON ${CLICKHOUSE_DATABASE}.t_policy_04841 FOR SELECT USING x = 2"
$CLICKHOUSE_CLIENT -q "SELECT s FROM v_definer_04841 SETTINGS ${qc}"

# Views first: a user cannot be dropped while it is the definer of one.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_definer_04841;
    DROP TABLE mv_open_04841;
    DROP TABLE v_definer_04841;
    DROP TABLE v_open_04841;
    DROP TABLE v_none_04841;
    DROP ROW POLICY policy_04841 ON ${CLICKHOUSE_DATABASE}.t_policy_04841;
    DROP ROW POLICY policy_target_04841 ON ${CLICKHOUSE_DATABASE}.target_policy_04841;
    DROP USER ${definer}, ${open_definer};
"

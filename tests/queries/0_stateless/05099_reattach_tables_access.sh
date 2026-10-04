#!/usr/bin/env bash
# Tags: long, no-flaky-check, no-random-detach, no-replicated-database, no-fasttest
# no-random-detach: test uses DETACH/ATTACH itself
# long: a comprehensive regression suite whose cumulative time across flaky-check reruns exceeds the
#       flaky-check budget, though each run is quick.
# no-flaky-check: the flaky check reruns a test 50 times; a single run of this suite already takes tens
#       of seconds on a sanitizer build, so the reruns hit the per-test timeout. The suite is
#       deterministic (it drives every `DETACH`/`ATTACH` itself and is `no-random-detach`), so there is
#       nothing for the flaky check to shake out here.
# no-fasttest: part of the `long` reattach suite; the fast test runs with `--no-long` and never runs it.

# The hook must preflight the whole authorization the internal `DETACH`/`ATTACH` pair needs - including
# the `TABLE ENGINE` grant - and the access a rejected query would have failed on anyway, so that a
# query the user is not allowed to run never leaves one of its tables detached.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

# A user with database-scoped `GRANT ALL ON db.*` has `DROP TABLE` and `CREATE TABLE` on the table, but not
# the global `TABLE ENGINE ON MergeTree` grant that the internal `ATTACH TABLE` requires when
# `access_control_improvements.table_engines_require_grant` is enabled (it is in the stateless test config).
# The reattach hook must account for the full `ATTACH` authorization; otherwise it would `DETACH` the table
# and then fail to re-attach it (with `ACCESS_DENIED` on the engine grant), leaving it detached. So the table
# must NOT be detached for such a user, and the query must succeed.
REATTACH_USER="user_reattach_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${REATTACH_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${REATTACH_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON ${CLICKHOUSE_DATABASE}.* TO ${REATTACH_USER}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_engine_grant"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_engine_grant (a UInt64) ENGINE = MergeTree ORDER BY a"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${REATTACH_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "SELECT * FROM t_reattach_engine_grant" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_engine_grant"; then
    echo "FAIL"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_engine_grant"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${REATTACH_USER}"

# The outer query's own access checks run only when its interpreter is constructed — after the reattach
# hook. The hook therefore preflights the access the query is going to check on the collected tables and
# skips the DETACH/ATTACH entirely when any of it is missing, so that a query rejected with ACCESS_DENIED
# stays side-effect free. A user with the DETACH/ATTACH grants (DROP TABLE, CREATE TABLE, TABLE ENGINE)
# but without SELECT on the table must get ACCESS_DENIED without any DETACH being logged.
ACC_USER="user_reattach_acc_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${ACC_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${ACC_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${ACC_USER}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_acc_1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_acc_2"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_acc_1 (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_acc_2 (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "GRANT DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_acc_1 TO ${ACC_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_acc_2 TO ${ACC_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${ACC_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "SELECT * FROM t_reattach_acc_1" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_acc_1"; then
    echo "FAIL (table was detached for an access-rejected query)"
else
    echo "OK"
fi

# The missing access may concern a table other than the one that would be detached: here the user may
# SELECT (and detach) t_reattach_acc_2 but lacks SELECT on t_reattach_acc_1, so the whole query fails
# with ACCESS_DENIED and neither table may be detached.
REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${ACC_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "SELECT * FROM t_reattach_acc_2 JOIN t_reattach_acc_1 USING a" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_acc"; then
    echo "FAIL (a table was detached for an access-rejected query)"
else
    echo "OK"
fi

# The missing access may also concern a table reached only through `IN`: here the user may SELECT (and
# detach) the FROM table t_reattach_acc_2 but lacks SELECT on the `IN` table t_reattach_acc_1, so the whole
# query fails with ACCESS_DENIED and neither table may be detached. This locks down that the `IN` table's
# required access is folded into the same preflight as FROM/JOIN tables.
REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${ACC_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "SELECT * FROM t_reattach_acc_2 WHERE a IN t_reattach_acc_1" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_acc"; then
    echo "FAIL (a table was detached for an access-rejected query)"
else
    echo "OK"
fi

# With SELECT granted on the table, the same user passes the preflight and the DETACH/ATTACH fires.
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_reattach_acc_1 TO ${ACC_USER}"
REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${ACC_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "SELECT * FROM t_reattach_acc_1" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_acc_1"; then
    echo "OK"
else
    echo "FAIL (table was not detached although all access is granted)"
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_acc_1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_acc_2"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${ACC_USER}"

# The missing access may also concern a table that is not a child AST node but a plain string field of the
# query. `CREATE OR REPLACE TABLE dst AS src` reads `src`'s structure, and `InterpreterCreateQuery` checks
# `SHOW_COLUMNS` on `src` (`create.as_database`/`create.as_table`). A user who can `DETACH`/`ATTACH` the
# existing destination `dst` (full table grants plus the engine grant) but lacks any access to the source
# `src` must fail with `ACCESS_DENIED` without `dst` being detached — the `AS` source has to be folded into
# the same preflight even though it lives outside the child AST.
CREATE_USER="user_reattach_create_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${CREATE_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${CREATE_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${CREATE_USER}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_create_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_create_src"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_create_dst (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_create_src (a UInt64) ENGINE = MergeTree ORDER BY a"
# Full table grants on the destination make it a genuine detach candidate; grant nothing on the source.
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON ${CLICKHOUSE_DATABASE}.t_reattach_create_dst TO ${CREATE_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${CREATE_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "CREATE OR REPLACE TABLE t_reattach_create_dst AS t_reattach_create_src" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_create_dst"; then
    echo "FAIL (destination detached for an access-rejected query)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_create_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_create_src"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${CREATE_USER}"

# The external target of `CREATE MATERIALIZED VIEW mv TO dst AS SELECT * FROM src` lives in `create.targets`,
# another plain (non-child-AST) carrier, and `InterpreterCreateQuery::getRequiredAccess` checks
# `SELECT | INSERT` on it. A user who can `DETACH`/`ATTACH` the source `src` but lacks access to `dst` must
# fail with `ACCESS_DENIED` without `src` being detached.
MV_USER="user_reattach_mv_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${MV_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${MV_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${MV_USER}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mv_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mv_dst"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_mv_src (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_mv_dst (a UInt64) ENGINE = MergeTree ORDER BY a"
# Full table grants on the source make it a genuine detach candidate; grant nothing on the target.
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON ${CLICKHOUSE_DATABASE}.t_reattach_mv_src TO ${MV_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE VIEW ON ${CLICKHOUSE_DATABASE}.t_reattach_mv TO ${MV_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${MV_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "CREATE MATERIALIZED VIEW t_reattach_mv TO t_reattach_mv_dst AS SELECT * FROM t_reattach_mv_src" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_mv_src"; then
    echo "FAIL (source detached for an access-rejected query)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS t_reattach_mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mv_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mv_dst"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${MV_USER}"

# The `TO dst` target of `CREATE MATERIALIZED VIEW ... AS SELECT` must also exist:
# `InterpreterCreateQuery::validateMaterializedViewColumnsAndEngine` resolves it through
# `DatabaseCatalog::getTable` before anything is created, so the query fails with `UNKNOWN_TABLE` and the
# source `src` must not be detached on the way — the existence preflight has to cover external targets too.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mv_to_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mv_to_dst"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_mv_to_src (a UInt64) ENGINE = MergeTree ORDER BY a"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} \
    --reattach_tables_before_query_execution=1 \
    --query "CREATE MATERIALIZED VIEW t_reattach_mv_to TO t_reattach_mv_to_missing_dst AS SELECT * FROM t_reattach_mv_to_src" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "UNKNOWN_TABLE"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_mv_to_src"; then
    echo "FAIL (source detached for a query failing on a missing target)"
else
    echo "OK"
fi

# Positive control: with the target present the same statement succeeds and the source is reattached.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_mv_to_dst (a UInt64) ENGINE = MergeTree ORDER BY a"
check_if_detached "CREATE MATERIALIZED VIEW t_reattach_mv_to TO t_reattach_mv_to_dst AS SELECT * FROM t_reattach_mv_to_src" "t_reattach_mv_to_src"

${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS t_reattach_mv_to"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mv_to_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mv_to_dst"

# External `TimeSeries` `SAMPLES`/`TAGS` targets are resolved and type-checked by
# `normalizeTimeSeriesDefinition` before the interpreter reads any source table, so
# `CREATE TABLE ts ENGINE = TimeSeries SAMPLES missing_samples AS src` fails with `UNKNOWN_TABLE`
# and must not detach `src` on the way. Because an existing target can still fail the type check
# there, any statement carrying such a target conservatively never triggers the `DETACH`/`ATTACH`.
# The source is itself a `TimeSeries` table: the columns of a `TimeSeries` table are always generated,
# so `getASCreateQuery` rejects an `AS` source that is not one before it ever gets to the target.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ts_src"
${CLICKHOUSE_CLIENT} --enable_time_series_table=1 -q "CREATE TABLE t_reattach_ts_src ENGINE = TimeSeries"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} \
    --reattach_tables_before_query_execution=1 \
    --enable_time_series_table=1 \
    --query "CREATE TABLE t_reattach_ts ENGINE = TimeSeries SAMPLES t_reattach_ts_missing_samples AS t_reattach_ts_src" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "UNKNOWN_TABLE"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_ts_src"; then
    echo "FAIL (source detached for a query failing on a missing TimeSeries target)"
else
    echo "OK"
fi

# Even a succeeding statement with an external `SAMPLES`/`TAGS` target is suppressed conservatively:
# the target itself must not be detached either.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ts_samples"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_ts_samples (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp)"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} \
    --reattach_tables_before_query_execution=1 \
    --enable_time_series_table=1 \
    --query "CREATE TABLE t_reattach_ts ENGINE = TimeSeries SAMPLES t_reattach_ts_samples" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_ts_samples"; then
    echo "FAIL (external TimeSeries target detached)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ts"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ts_samples"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ts_src"

# `ALTER TABLE dst REPLACE PARTITION ... FROM src` needs `SELECT` on the source `src` (see
# `InterpreterAlterQuery::getRequiredAccessForCommand`), which is kept in the command's `from_*` string
# fields, not in a child AST node. A user who can `DETACH`/`ATTACH` the target `dst` but lacks `SELECT` on
# `src` must fail with `ACCESS_DENIED` without `dst` being detached — the `from_*`/`to_*` tables have to be
# folded into the same preflight. (Access is checked before partition validation, so no data is needed.)
ALTER_USER="user_reattach_alter_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${ALTER_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${ALTER_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${ALTER_USER}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_alter_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_alter_src"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_alter_dst (a UInt64) ENGINE = MergeTree PARTITION BY a ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_alter_src (a UInt64) ENGINE = MergeTree PARTITION BY a ORDER BY a"
# Full table grants on the target make it a genuine detach candidate; grant nothing on the source.
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON ${CLICKHOUSE_DATABASE}.t_reattach_alter_dst TO ${ALTER_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${ALTER_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "ALTER TABLE t_reattach_alter_dst REPLACE PARTITION 1 FROM t_reattach_alter_src" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_alter_dst"; then
    echo "FAIL (target detached for an access-rejected query)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_alter_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_alter_src"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${ALTER_USER}"

# The required access can also depend on execution-time details of the same statement. `InterpreterUpdateQuery`
# governs the lightweight-delete form `UPDATE ... SET _row_exists = 0` (where `_row_exists` is the MergeTree
# virtual marker) by `ALTER_DELETE`, not `ALTER_UPDATE`. A user granted `ALTER_UPDATE` plus the internal
# `DETACH`/`ATTACH` grants but not `ALTER_DELETE` must fail with `ACCESS_DENIED` without the table being
# detached — so the preflight over-requires all table-level flags for `UPDATE`, matching `ALTER`.
UPDATE_USER="user_reattach_update_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${UPDATE_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${UPDATE_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${UPDATE_USER}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_update"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_update (a UInt64) ENGINE = MergeTree ORDER BY a"
# Everything except `ALTER DELETE`: the detach/attach grants, plus `ALTER UPDATE` so the query would pass a
# preflight that only checked `ALTER_UPDATE`.
${CLICKHOUSE_CLIENT} -q "GRANT DROP TABLE, CREATE TABLE, ALTER UPDATE ON ${CLICKHOUSE_DATABASE}.t_reattach_update TO ${UPDATE_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${UPDATE_USER}" \
    --reattach_tables_before_query_execution=1 --enable_lightweight_update=1 \
    --query "UPDATE t_reattach_update SET _row_exists = 0 WHERE a = 1" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_update"; then
    echo "FAIL (table detached for an access-rejected query)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_update"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${UPDATE_USER}"

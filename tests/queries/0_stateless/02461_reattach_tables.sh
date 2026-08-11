#!/usr/bin/env bash
# Tags: long, no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself
# long: comprehensive regression suite (RBAC users, BACKUP/RESTORE, many DETACH/ATTACH cycles) whose
#       cumulative time across flaky-check reruns exceeds the flaky-check budget, though each run is quick

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

MY_CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=trace/g')

# Run the query and capture both the client exit code and its output (including trace logs).
# A non-zero client exit code means the query itself failed, which must be reported as FAIL by the
# callers instead of being silently treated as "the table was not detached".
function check_if_detached_impl()
{
    query="$1"
    REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} \
        --reattach_tables_before_query_execution=1  \
        --query "$query" 2>&1)
    REATTACH_STATUS=$?
}

function check_if_detached()
{
    check_if_detached_impl "$1" "$2"
    if [ "$REATTACH_STATUS" -ne 0 ]; then
        echo "FAIL (client error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.$2"; then
        echo "OK"
    else
        echo "FAIL"
    fi
}

function check_if_not_detached()
{
    check_if_detached_impl "$1" "$2"
    if [ "$REATTACH_STATUS" -ne 0 ]; then
        echo "FAIL (client error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.$2"; then
        echo "FAIL"
    else
        echo "OK"
    fi
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_2"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_1 (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_2 (a UInt64) ENGINE = MergeTree ORDER BY a"

check_if_detached "INSERT INTO t_reattach_1 VALUES (1)" "t_reattach_1"

check_if_detached "SELECT * FROM t_reattach_1" "t_reattach_1"
check_if_detached "SELECT * FROM t_reattach_1 JOIN t_reattach_2 USING a" "t_reattach_1"
check_if_detached "SELECT * FROM t_reattach_1 JOIN t_reattach_2 USING a" "t_reattach_2"

# `IN table` / `GLOBAL IN table` keep the right-hand-side table as a bare identifier outside the FROM/JOIN
# table expressions, but it is still a real table the query reads, so both the FROM table and the IN table
# must be detached. (A subquery right-hand side is covered by the CTE/subquery cases below.)
check_if_detached "SELECT * FROM t_reattach_1 WHERE a IN t_reattach_2" "t_reattach_1"
check_if_detached "SELECT * FROM t_reattach_1 WHERE a IN t_reattach_2" "t_reattach_2"
check_if_detached "SELECT * FROM t_reattach_1 WHERE a GLOBAL IN t_reattach_2" "t_reattach_2"

check_if_detached "INSERT INTO t_reattach_2 SELECT * FROM t_reattach_1" "t_reattach_1"
check_if_detached "INSERT INTO t_reattach_2 SELECT * FROM t_reattach_1" "t_reattach_2"

check_if_detached "EXISTS TABLE t_reattach_1" "t_reattach_1"
check_if_detached "SHOW CREATE TABLE t_reattach_1" "t_reattach_1"

# `BACKUP` is entirely out of the hook's scope, including the explicit `BACKUP TABLE t` form that names the
# local table it reads: `BackupsWorker::BackupStarter::doBackup` opens and validates the destination
# (`openBackupForWriting`) before it builds `BackupEntriesCollector`, so a backup with an invalid
# destination fails before the source table is ever read — detaching the source up front would give such a
# failing query a `DETACH`/`ATTACH` side effect on a table it never touches. `BACKUP DATABASE` and
# `BACKUP ALL` additionally name no explicit table and expand into per-table work only during execution
# (`RESTORE` is out of scope too — see the `RESTORE` cases below). Use a unique per-run destination so
# parallel runs and flaky-check reruns never collide on an existing backup path.
BACKUP_SUFFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}_$RANDOM"
check_if_not_detached "BACKUP TABLE t_reattach_1 TO Disk('backups', '${BACKUP_SUFFIX}_table')" "t_reattach_1"
check_if_not_detached "BACKUP DATABASE ${CLICKHOUSE_DATABASE} TO Disk('backups', '${BACKUP_SUFFIX}_db')" "t_reattach_1"

# The focused regression for the failing-backup case: the destination already holds a backup, so the second
# backup to the same destination fails with BACKUP_ALREADY_EXISTS in `openBackupForWriting` before ever
# reading the source table — which therefore must NOT be detached.
check_if_detached_impl "BACKUP TABLE t_reattach_1 TO Disk('backups', '${BACKUP_SUFFIX}_table')" "t_reattach_1"
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (backup to an already existing destination unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "BACKUP_ALREADY_EXISTS"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_1"; then
    echo "FAIL (source table was detached for a backup that fails before reading it)"
else
    echo "OK"
fi

# `RESTORE` is entirely out of the hook's scope, including the explicit `RESTORE TABLE old AS new` form:
# `RestorerFromBackup::run` first resolves the source objects inside the backup
# (`findDatabasesAndTablesInBackup`) and only later touches the local destination, so a restore whose source
# entry is missing from the backup fails without ever touching an existing destination table — detaching the
# destination up front would give a failing query a `DETACH`/`ATTACH` side effect on a table it never
# touches. Hence a restore detaches nothing: neither a local table whose name matches the in-backup SOURCE
# name (`t_reattach_1` here), nor an existing destination table.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_restored"
check_if_not_detached "RESTORE TABLE t_reattach_1 AS t_reattach_restored FROM Disk('backups', '${BACKUP_SUFFIX}_table')" "t_reattach_1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_restored"

# An existing (empty) destination of a restore that succeeds is not detached either.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_restored (a UInt64) ENGINE = MergeTree ORDER BY a"
check_if_not_detached "RESTORE TABLE t_reattach_1 AS t_reattach_restored FROM Disk('backups', '${BACKUP_SUFFIX}_table')" "t_reattach_restored"

# The focused regression for the failing-restore case: the destination exists, but the SOURCE is absent
# from the backup, so the restore fails with BACKUP_ENTRY_NOT_FOUND in `findDatabasesAndTablesInBackup`
# before ever touching the destination — which therefore must NOT be detached.
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t_reattach_restored"
check_if_detached_impl "RESTORE TABLE t_reattach_missing_src AS t_reattach_restored FROM Disk('backups', '${BACKUP_SUFFIX}_table')" "t_reattach_restored"
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (restore of a source missing from the backup unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "BACKUP_ENTRY_NOT_FOUND"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_restored"; then
    echo "FAIL (existing destination was detached for a restore that fails before touching it)"
else
    echo "OK"
fi
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_restored"

# A `... TEMPORARY TABLE t` statement targets a session-local temporary table, not the persistent table of
# the same (unqualified) name. With no temporary `t_reattach_1` in the session, these queries do not touch
# the persistent `t_reattach_1`, so the reattach hook must NOT detach it. `EXISTS TEMPORARY TABLE` returns 0
# and `DROP TEMPORARY TABLE IF EXISTS` is a no-op, so both succeed without a temporary table present.
check_if_not_detached "EXISTS TEMPORARY TABLE t_reattach_1" "t_reattach_1"
check_if_not_detached "DROP TEMPORARY TABLE IF EXISTS t_reattach_1" "t_reattach_1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_2"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_1 (a UInt64) ENGINE = Memory"

check_if_not_detached "INSERT INTO t_reattach_1 VALUES (55)" "t_reattach_1"
check_if_not_detached "SELECT * FROM t_reattach_1" "t_reattach_1"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM t_reattach_1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_1"

${CLICKHOUSE_CLIENT} --reattach_tables_before_query_execution=1 -q "SELECT number FROM system.numbers LIMIT 1"
${CLICKHOUSE_CLIENT} --reattach_tables_before_query_execution=1 -q "SELECT number FROM system.numbers LIMIT 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_cte"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_cte (a UInt64) ENGINE = MergeTree ORDER BY a"

# A real CTE (WITH name AS (subquery)) shadows a table with the same name, so the table is not used.
check_if_not_detached "WITH t_reattach_cte AS (SELECT 1) SELECT * FROM t_reattach_cte" "t_reattach_cte"

# A scalar WITH alias does NOT shadow a table name in FROM: `WITH (SELECT 1) AS t_reattach_cte SELECT * FROM
# t_reattach_cte` reads the real table, so it is detached.
check_if_detached "WITH (SELECT 1) AS t_reattach_cte SELECT * FROM t_reattach_cte" "t_reattach_cte"

# A CTE's own definition body may reference a real table with the same name (only the CTE currently being
# resolved is hidden), so the real table is read inside the body and detached.
check_if_detached "WITH t_reattach_cte AS (SELECT * FROM t_reattach_cte) SELECT * FROM t_reattach_cte" "t_reattach_cte"

# A CTE defined only in a nested subquery must NOT shadow the same name in an outer FROM clause.
check_if_detached "SELECT * FROM t_reattach_cte WHERE a IN (WITH t_reattach_cte AS (SELECT 1) SELECT * FROM t_reattach_cte)" "t_reattach_cte"

# A recursive CTE resolves its self-reference through the recursive temporary table, not a real table with the
# same name, so the real table is NOT read inside the recursive member and must NOT be detached.
check_if_not_detached "WITH RECURSIVE t_reattach_cte AS (SELECT toUInt64(1) AS a UNION ALL SELECT a + 1 FROM t_reattach_cte WHERE a < 2) SELECT * FROM t_reattach_cte" "t_reattach_cte"

# An expression alias, unlike a FROM reference, DOES shadow a same-named table on the bare-identifier right-hand
# side of `IN`: the analyzer resolves `t_reattach_cte` there to the alias, so the real table is never read and
# must NOT be detached. Both the `WITH expr AS alias` and the `SELECT expr AS alias` forms behave this way.
check_if_not_detached "WITH (1, 2) AS t_reattach_cte SELECT 1 IN t_reattach_cte" "t_reattach_cte"
check_if_not_detached "SELECT (1, 2) AS t_reattach_cte, 1 IN t_reattach_cte" "t_reattach_cte"

# A recursive CTE's NON-RECURSIVE seed term (the first UNION member) is resolved before the recursive temporary
# table exists, so a same-named real table read by the seed term IS read by the query and must be detached.
# Only the recursive members (after the first) resolve the name through the recursive temporary table.
check_if_detached "WITH RECURSIVE t_reattach_cte AS (SELECT a FROM t_reattach_cte UNION ALL SELECT a + 1 FROM t_reattach_cte WHERE a < 2) SELECT * FROM t_reattach_cte" "t_reattach_cte"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_cte"

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
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ts_src"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_ts_src (a UInt64) ENGINE = MergeTree ORDER BY a"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} \
    --reattach_tables_before_query_execution=1 \
    --allow_experimental_time_series_table=1 \
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
    --allow_experimental_time_series_table=1 \
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

# A required table reference that does not exist means the query itself is going to fail (with
# UNKNOWN_TABLE, UNKNOWN_IDENTIFIER under the analyzer for the `IN` form, or CANNOT_GET_CREATE_TABLE_QUERY
# for the `CREATE ... AS src` form), so the hook must skip entirely: the references that do exist must NOT
# be detached first. Covers FROM/JOIN, the `IN table` form, and the `CREATE ... AS src` string field.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_unres"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_unres (a UInt64) ENGINE = MergeTree ORDER BY a"

function check_fails_without_detach()
{
    check_if_detached_impl "$1" "$2"
    if [ "$REATTACH_STATUS" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$REATTACH_OUTPUT" | grep -q -e "UNKNOWN_TABLE" -e "UNKNOWN_IDENTIFIER" -e "CANNOT_GET_CREATE_TABLE_QUERY"; then
        echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.$2"; then
        echo "FAIL (table was detached for a query referencing a missing table)"
    else
        echo "OK"
    fi
}

check_fails_without_detach "SELECT * FROM t_reattach_unres JOIN t_reattach_unres_missing USING a" "t_reattach_unres"
check_fails_without_detach "SELECT * FROM t_reattach_unres WHERE a IN t_reattach_unres_missing" "t_reattach_unres"
check_fails_without_detach "CREATE OR REPLACE TABLE t_reattach_unres AS t_reattach_unres_missing" "t_reattach_unres"

# An OPTIONAL miss must not disable the hook: the target of a plain `CREATE ... AS src` does not exist yet
# (that is the point of the query), and the resolvable source is still detached.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_unres_new"
check_if_detached "CREATE TABLE t_reattach_unres_new AS t_reattach_unres" "t_reattach_unres"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_unres_new"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_unres"

# A kind-specific metadata probe on a name that resolves to a plain table never touches that table's
# storage: `EXISTS VIEW` / `EXISTS DICTIONARY` answer 0, and `SHOW CREATE VIEW` / `SHOW CREATE DICTIONARY`
# fail with BAD_ARGUMENTS ("... is not a VIEW" / "... is not a DICTIONARY"). The reattach hook must NOT
# detach the unrelated table in either case.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_kind"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_kind (a UInt64) ENGINE = MergeTree ORDER BY a"

check_if_not_detached "EXISTS VIEW t_reattach_kind" "t_reattach_kind"
check_if_not_detached "EXISTS DICTIONARY t_reattach_kind" "t_reattach_kind"

function check_fails_kind_without_detach()
{
    local expected_error="${3:-BAD_ARGUMENTS}"
    check_if_detached_impl "$1" "$2"
    if [ "$REATTACH_STATUS" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$REATTACH_OUTPUT" | grep -q "$expected_error"; then
        echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.$2"; then
        echo "FAIL (table was detached for a kind-mismatched metadata query)"
    else
        echo "OK"
    fi
}

check_fails_kind_without_detach "SHOW CREATE VIEW t_reattach_kind" "t_reattach_kind"
check_fails_kind_without_detach "SHOW CREATE DICTIONARY t_reattach_kind" "t_reattach_kind"

# The kind-specific `DROP`/`DETACH` forms fail the same way: `InterpreterDropQuery` throws INCORRECT_QUERY
# on an `is_view`/`is_dictionary` mismatch before touching the table's storage, so the hook must not
# detach the table either.
check_fails_kind_without_detach "DROP VIEW t_reattach_kind" "t_reattach_kind" "INCORRECT_QUERY"
check_fails_kind_without_detach "DETACH VIEW t_reattach_kind" "t_reattach_kind" "INCORRECT_QUERY"
check_fails_kind_without_detach "DROP DICTIONARY t_reattach_kind" "t_reattach_kind" "INCORRECT_QUERY"
check_fails_kind_without_detach "DETACH DICTIONARY t_reattach_kind" "t_reattach_kind" "INCORRECT_QUERY"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_kind"

# A session temporary table with the same name as a persistent one must affect the hook exactly as it
# affects the query itself. Carriers whose interpreter resolves temporary tables first (SELECT,
# SHOW CREATE TABLE) target the temporary table, so the persistent one must NOT be detached. Carriers
# whose interpreter looks the name up only in the persistent catalog (EXISTS TABLE, CREATE ... AS src)
# use the persistent table, so it must still be detached — the temporary hit must not hide it from the
# collector.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_shadow"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_shadow (a UInt64) ENGINE = MergeTree ORDER BY a"

check_if_not_detached "CREATE TEMPORARY TABLE t_reattach_shadow (a UInt64); SELECT * FROM t_reattach_shadow" "t_reattach_shadow"
check_if_not_detached "CREATE TEMPORARY TABLE t_reattach_shadow (a UInt64); SHOW CREATE TABLE t_reattach_shadow FORMAT Null" "t_reattach_shadow"
check_if_detached "CREATE TEMPORARY TABLE t_reattach_shadow (a UInt64); EXISTS TABLE t_reattach_shadow" "t_reattach_shadow"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_shadow_new"
check_if_detached "CREATE TEMPORARY TABLE t_reattach_shadow (a UInt64); CREATE TABLE t_reattach_shadow_new AS t_reattach_shadow" "t_reattach_shadow"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_shadow_new"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_shadow"

# Index-management statements travel through `ASTQueryWithTableAndOutput` like `ALTER TABLE`, but not all of
# them reach the table. `InterpreterCreateIndexQuery` rewrites `CREATE INDEX` to `ALTER TABLE ... ADD INDEX`
# only after `validateCreateIndexQuery` accepts it: `CREATE UNIQUE INDEX` throws unless
# `create_index_ignore_unique` is set, and `CREATE INDEX` without a `TYPE` either throws or (with
# `allow_create_index_without_type`) is a no-op. Those shapes must NOT detach the table, while the shapes
# that really rewrite — and `DROP INDEX`, which always rewrites — must.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_index"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_index (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a"

check_fails_kind_without_detach "CREATE UNIQUE INDEX idx_u ON t_reattach_index (b) TYPE minmax GRANULARITY 1" "t_reattach_index" "NOT_IMPLEMENTED"
check_fails_kind_without_detach "CREATE INDEX idx_no_type ON t_reattach_index (b)" "t_reattach_index" "INCORRECT_QUERY"
check_if_not_detached "SET allow_create_index_without_type = 1; CREATE INDEX idx_no_type ON t_reattach_index (b)" "t_reattach_index"

check_if_detached "SET create_index_ignore_unique = 1; CREATE UNIQUE INDEX idx_u ON t_reattach_index (b) TYPE minmax GRANULARITY 1" "t_reattach_index"
check_if_detached "CREATE INDEX idx_t ON t_reattach_index (b) TYPE minmax GRANULARITY 1" "t_reattach_index"
check_if_detached "DROP INDEX idx_t ON t_reattach_index" "t_reattach_index"

# `CREATE`/`DROP HYPOTHETICAL INDEX` never mutates the table: the interpreter only reads its metadata and
# updates the session-local hypothetical-index store, so the hook must not detach the table for them.
check_if_not_detached "CREATE HYPOTHETICAL INDEX idx_h ON t_reattach_index (b) TYPE minmax GRANULARITY 1" "t_reattach_index"
check_if_not_detached "DROP HYPOTHETICAL INDEX IF EXISTS idx_h ON t_reattach_index" "t_reattach_index"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_index"

# A `CREATE` statement stops on its own destination before it ever reads the tables it selects from:
# `InterpreterCreateQuery::execute` checks the destination-side access first, and the plain-create path
# then short-circuits on a taken destination name. Neither shape may detach the source.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_src"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_dest_src (a UInt64) ENGINE = MergeTree ORDER BY a"

# 1. Destination access. The user has full grants on the source (so it is a genuine detach candidate) but
# no `CREATE VIEW` on the destination, so `CREATE VIEW v AS SELECT * FROM src` fails with `ACCESS_DENIED`.
DEST_USER="user_reattach_dest_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${DEST_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${DEST_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON ${CLICKHOUSE_DATABASE}.t_reattach_dest_src TO ${DEST_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${DEST_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "CREATE VIEW t_reattach_dest_view AS SELECT * FROM t_reattach_dest_src" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_dest_src"; then
    echo "FAIL (source detached for an access-rejected query)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS t_reattach_dest_view"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${DEST_USER}"

# 2. Taken destination name. `CREATE ... IF NOT EXISTS` over an existing destination is a pure no-op that
# never runs the `SELECT`, and the plain form fails with `TABLE_ALREADY_EXISTS` before it — in both cases
# the source must stay attached. The same statement over a free destination name does detach the source.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_taken"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_dest_taken (a UInt64) ENGINE = MergeTree ORDER BY a"

check_if_not_detached "CREATE TABLE IF NOT EXISTS t_reattach_dest_taken ENGINE = MergeTree ORDER BY a AS SELECT * FROM t_reattach_dest_src" "t_reattach_dest_src"
check_if_not_detached "CREATE VIEW IF NOT EXISTS t_reattach_dest_taken AS SELECT * FROM t_reattach_dest_src" "t_reattach_dest_src"
check_fails_kind_without_detach "CREATE TABLE t_reattach_dest_taken ENGINE = MergeTree ORDER BY a AS SELECT * FROM t_reattach_dest_src" "t_reattach_dest_src" "TABLE_ALREADY_EXISTS"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_free"
check_if_detached "CREATE TABLE t_reattach_dest_free ENGINE = MergeTree ORDER BY a AS SELECT * FROM t_reattach_dest_src" "t_reattach_dest_src"

# 3. Stub `ATTACH` with dropped clauses. An `ATTACH` without an engine and a column list applies the table
# definition from stored metadata and rejects any user-supplied clause it would otherwise silently drop
# with `BAD_ARGUMENTS` before reading any source or target table. The materialized-view form
# `ATTACH MATERIALIZED VIEW mv TO dst AS SELECT ... FROM src` is the parseable shape of this rejection
# that names other live tables — both its external `TO` target and its `SELECT` source must stay
# attached on the way to it.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_attach_dst"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_attach_dst (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_reattach_attach_dst"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mv_target"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_mv_target (a UInt64) ENGINE = MergeTree ORDER BY a"

check_fails_kind_without_detach "ATTACH MATERIALIZED VIEW t_reattach_attach_dst TO t_reattach_mv_target AS SELECT a FROM t_reattach_dest_src" "t_reattach_mv_target" "BAD_ARGUMENTS"
check_fails_kind_without_detach "ATTACH MATERIALIZED VIEW t_reattach_attach_dst TO t_reattach_mv_target AS SELECT a FROM t_reattach_dest_src" "t_reattach_dest_src" "BAD_ARGUMENTS"

# The rejections must have left no side effects behind: the proper stub `ATTACH` still works.
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_reattach_attach_dst"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_reattach_attach_dst"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_attach_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mv_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_free"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_taken"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_src"

# A `MergeTree` table whose metadata carries any TTL is skipped: the internal `DETACH TABLE ... SYNC`
# cancels selected-but-not-started background TTL merges, and every such cancellation leaks a
# `max_number_of_merges_with_ttl_in_pool` slot until server restart (see the comment in
# `reattachTablesUsedInQuery` and https://github.com/ClickHouse/ClickHouse/pull/111925).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ttl"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_ttl (a UInt64, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + INTERVAL 1 DAY"
check_if_not_detached "SELECT * FROM t_reattach_ttl FORMAT Null" "t_reattach_ttl"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ttl"

# A table with an Outdated part that no Active part covers is skipped: the `DETACH`/`ATTACH` cycle
# reloads the parts from disk and would resurrect that part as Active. `ALTER TABLE ... DETACH PART`
# leaves such a part behind — the empty covering part it creates is immediately dropped from the
# working set but stays on disk until the asynchronous cleanup.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_uncovered"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_uncovered (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_uncovered VALUES (1)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_uncovered VALUES (2)"
check_if_detached "SELECT * FROM t_reattach_uncovered FORMAT Null" "t_reattach_uncovered"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_reattach_uncovered DETACH PART 'all_1_1_0'"
check_if_not_detached "SELECT * FROM t_reattach_uncovered FORMAT Null" "t_reattach_uncovered"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_uncovered"

# A replacing form validates the new definition before the replacement path touches the existing
# destination: the populating `SELECT` is analyzed by `getTablePropertiesAndNormalizeCreateQuery`
# and an `AS src` source is validated by `setEngine`, so `CREATE OR REPLACE TABLE dst AS SELECT missing_col
# FROM src` (or `... AS view_src`) fails with `dst` untouched — and a source-less form can be rejected
# there as incomplete too (`CREATE OR REPLACE TABLE dst` with no column list). The hook cannot predict
# whether that validation passes, so every replacing destination must stay out of scope — even for a
# statement that goes on to succeed.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_repl_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_repl_src"
${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS t_reattach_repl_view"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_repl_dst (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_repl_src (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE VIEW t_reattach_repl_view AS SELECT a FROM t_reattach_repl_src"

check_fails_kind_without_detach "CREATE OR REPLACE TABLE t_reattach_repl_dst ENGINE = MergeTree ORDER BY a AS SELECT missing_col FROM t_reattach_repl_src" "t_reattach_repl_dst" "UNKNOWN_IDENTIFIER"
check_fails_kind_without_detach "CREATE OR REPLACE TABLE t_reattach_repl_dst AS t_reattach_repl_view" "t_reattach_repl_dst" "INCORRECT_QUERY"
check_fails_kind_without_detach "CREATE OR REPLACE TABLE t_reattach_repl_dst" "t_reattach_repl_dst" "INCORRECT_QUERY"

# The failing statements above must not have replaced or lost the destination.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_reattach_repl_dst"

check_if_not_detached "CREATE OR REPLACE TABLE t_reattach_repl_dst ENGINE = MergeTree ORDER BY a AS SELECT a FROM t_reattach_repl_src" "t_reattach_repl_dst"
check_if_not_detached "CREATE OR REPLACE TABLE t_reattach_repl_dst (a UInt64) ENGINE = MergeTree ORDER BY a" "t_reattach_repl_dst"

${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS t_reattach_repl_view"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_repl_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_repl_dst"

# A temporary-table `CREATE` rejected on its syntax alone — a database-qualified temporary
# (`BAD_DATABASE_FOR_TEMPORARY_TABLE`) or a temporary created `ON CLUSTER` (`INCORRECT_QUERY`) — is thrown
# out at the very top of `InterpreterCreateQuery::createTable`, before the populating `SELECT` or the
# `AS src` structure source is ever analyzed, so the hook must not reattach those sources on the way to
# the rejection. The same statement without the rejected clause does read the source and keeps detaching it.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_tmp_src"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_tmp_src (a UInt64) ENGINE = MergeTree ORDER BY a"

check_fails_kind_without_detach "CREATE TEMPORARY TABLE ${CLICKHOUSE_DATABASE}.t_reattach_tmp ENGINE = Memory AS SELECT * FROM t_reattach_tmp_src" "t_reattach_tmp_src" "BAD_DATABASE_FOR_TEMPORARY_TABLE"
check_fails_kind_without_detach "CREATE TEMPORARY TABLE t_reattach_tmp ON CLUSTER test_shard_localhost ENGINE = Memory AS SELECT * FROM t_reattach_tmp_src" "t_reattach_tmp_src" "INCORRECT_QUERY"

check_if_detached "CREATE TEMPORARY TABLE t_reattach_tmp ENGINE = Memory AS SELECT * FROM t_reattach_tmp_src" "t_reattach_tmp_src"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_tmp_src"

# An `ON CLUSTER` statement is out of the hook's scope entirely: on the initiator the interpreter
# delegates to `executeDDLQueryOnCluster` before performing any local table operation (the local host may
# not even be in the target cluster), and the real per-host executions replayed by the `DDLWorker` are not
# `INITIAL_QUERY`, so neither side may reattach. The same statement without the `ON CLUSTER` clause does
# touch the local table and keeps detaching it.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_oc"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_oc (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_oc VALUES (1)"
check_if_not_detached "OPTIMIZE TABLE t_reattach_oc ON CLUSTER test_shard_localhost FINAL" "t_reattach_oc"
check_if_detached "OPTIMIZE TABLE t_reattach_oc FINAL" "t_reattach_oc"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_oc"

# Tables of a `URL` database are resolved dynamically, and the resolution itself is not free of side
# effects: it infers the table structure from the data, and for a `file://` URL it requires the read
# source grant already in `tryGetTable` (see `DatabaseURL::getTableImpl`). The hook must reject the
# database (it does not support detaching tables) before resolving any table of it: `EXISTS TABLE`
# requires only `SHOW TABLES`, so for a user without the read source grant the hook's eligibility
# probe would otherwise fail the query with `ACCESS_DENIED`.
URL_DB="db_reattach_url_${CLICKHOUSE_DATABASE}"
URL_USER="user_reattach_url_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${URL_DB}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${URL_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${URL_DB} ENGINE = URL('file://')"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${URL_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW TABLES ON ${URL_DB}.* TO ${URL_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${URL_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "EXISTS TABLE ${URL_DB}.\`${CLICKHOUSE_USER_FILES_UNIQUE}/02461_data.csv\`" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE"; then
    echo "FAIL (a URL database table was detached)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP USER ${URL_USER}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${URL_DB}"

# The engine of an engine-less `CREATE ... AS SELECT` is inferred from `default_table_engine`
# (`default_temporary_table_engine` for a temporary table) by `setEngine`, and
# `getTablePropertiesAndNormalizeCreateQuery` then checks the `TABLE ENGINE` grant on the inferred
# engine before the populating `SELECT` is analyzed — exactly as it does for an explicit engine.
# A user who may create the destination but lacks that grant is stopped there, so the source must
# stay attached on the way to the `ACCESS_DENIED`. With the grant in place, the same statement over
# a free destination name no longer stops and must still detach its source.
IMPLICIT_USER="user_reattach_implicit_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${IMPLICIT_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_implicit_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_implicit_src"

# The source is a `Log` table so that the user's missing `TABLE ENGINE ON MergeTree` / `ON Memory`
# grant affects only the inferred destination engine: the hook's own reattach preflight on the source
# requires the grant for the source's engine, which the user has.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_implicit_src (a UInt64) ENGINE = Log"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_implicit_src VALUES (1)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${IMPLICIT_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON ${CLICKHOUSE_DATABASE}.* TO ${IMPLICIT_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE ON *.* TO ${IMPLICIT_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON Log TO ${IMPLICIT_USER}"

function check_implicit_engine_denied()
{
    REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${IMPLICIT_USER}" \
        --reattach_tables_before_query_execution=1 \
        --default_table_engine=MergeTree --default_temporary_table_engine=Memory \
        --query "$1" 2>&1)
    REATTACH_STATUS=$?
    if [ "$REATTACH_STATUS" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
        echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_implicit_src"; then
        echo "FAIL (source detached for an engine-rejected query)"
    else
        echo "OK"
    fi
}

check_implicit_engine_denied "CREATE TABLE t_reattach_implicit_dst AS SELECT * FROM t_reattach_implicit_src"
check_implicit_engine_denied "CREATE TEMPORARY TABLE t_reattach_implicit_tmp AS SELECT * FROM t_reattach_implicit_src"

${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${IMPLICIT_USER}"
REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${IMPLICIT_USER}" \
    --reattach_tables_before_query_execution=1 \
    --default_table_engine=MergeTree --create_table_empty_primary_key_by_default=1 \
    --query "CREATE TABLE t_reattach_implicit_dst AS SELECT * FROM t_reattach_implicit_src" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_implicit_src"; then
    echo "OK"
else
    echo "FAIL (source not detached for a granted implicit-engine statement)"
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_implicit_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_implicit_src"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${IMPLICIT_USER}"

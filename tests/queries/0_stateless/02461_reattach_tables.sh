#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself

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

# `BACKUP`/`RESTORE` cover only explicit `TABLE` elements. `BACKUP TABLE t` names the local table it reads,
# so the reattach hook detaches it. `BACKUP DATABASE` (and `BACKUP`/`RESTORE ALL`, and the `RESTORE`
# equivalents) name no explicit table and expand into per-table work only during execution, so they are
# deliberately out of scope and must NOT detach any table. Use a unique per-run destination so parallel
# runs and flaky-check reruns never collide on an existing backup path.
BACKUP_SUFFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}_$RANDOM"
check_if_detached "BACKUP TABLE t_reattach_1 TO Disk('backups', '${BACKUP_SUFFIX}_table')" "t_reattach_1"
check_if_not_detached "BACKUP DATABASE ${CLICKHOUSE_DATABASE} TO Disk('backups', '${BACKUP_SUFFIX}_db')" "t_reattach_1"

# `RESTORE TABLE old AS new` writes the local DESTINATION table (`new`); the source object name `old` lives
# only inside the backup. The collector therefore resolves the destination name for `RESTORE`, so a local
# table whose name matches the in-backup SOURCE name is unrelated to the restore and must NOT be detached
# (and the fresh destination does not exist yet, so nothing is detached there either). Restore the backup
# taken just above under a new name and confirm the source-named local table `t_reattach_1` stays attached —
# this locks down the `backup->kind == RESTORE` branch that must use `new_table_name`, not `table_name`.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_restored"
check_if_not_detached "RESTORE TABLE t_reattach_1 AS t_reattach_restored FROM Disk('backups', '${BACKUP_SUFFIX}_table')" "t_reattach_1"
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

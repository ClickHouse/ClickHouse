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

# Which tables the reattach hook picks up for ordinary reads and writes, and the statements that name a
# table without reading it: `BACKUP`/`RESTORE`, `... TEMPORARY TABLE`, and CTE/alias name shadowing.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

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

# ... but only an alias the `IN` can actually see. Alias visibility is scoped: an alias declared in a CHILD
# scope is invisible to the enclosing query, so it must NOT hide the real table there. Both a sibling
# subquery in the same clause and a subquery nested under the `IN` argument itself declare the name out of
# reach of the outer reference, which therefore still reads the table and must detach it.
check_if_detached "SELECT 1 WHERE 1 IN t_reattach_cte AND 1 = (SELECT 1 AS t_reattach_cte)" "t_reattach_cte"
check_if_detached "SELECT 1 WHERE (SELECT 1 AS t_reattach_cte) IN t_reattach_cte" "t_reattach_cte"

# A recursive CTE's NON-RECURSIVE seed term (the first UNION member) is resolved before the recursive temporary
# table exists, so a same-named real table read by the seed term IS read by the query and must be detached.
# Only the recursive members (after the first) resolve the name through the recursive temporary table.
check_if_detached "WITH RECURSIVE t_reattach_cte AS (SELECT a FROM t_reattach_cte UNION ALL SELECT a + 1 FROM t_reattach_cte WHERE a < 2) SELECT * FROM t_reattach_cte" "t_reattach_cte"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_cte"

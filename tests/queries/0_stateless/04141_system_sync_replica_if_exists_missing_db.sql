-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103629
-- `SYSTEM SYNC REPLICA <db>.<tbl> IF EXISTS` must silently succeed when the
-- database does not exist, the same way it does when the table does not exist.
-- Before the fix, `InterpreterSystemQuery::execute` called `resolveStorageID`
-- unconditionally, which threw `UNKNOWN_DATABASE` before the per-handler
-- `if_exists` check at `syncReplica` was ever consulted.

DROP DATABASE IF EXISTS missing_db_04141;

-- Case 1: missing table in the implicit current database — already worked.
SELECT 'case1';
SYSTEM SYNC REPLICA missing_table_04141 IF EXISTS;

-- Case 2: missing database — used to throw `UNKNOWN_DATABASE`, now silently succeeds.
SELECT 'case2';
SYSTEM SYNC REPLICA missing_db_04141.missing_table_04141 IF EXISTS;

-- Case 3: missing table in an explicit, existing database — already worked.
SELECT 'case3';
SYSTEM SYNC REPLICA default.missing_table_04141 IF EXISTS;

SELECT 'done';

-- Sanity checks: without `IF EXISTS`, the same queries still report the right errors.
SELECT 'check_no_if_exists_missing_table';
SYSTEM SYNC REPLICA missing_table_04141; -- { serverError UNKNOWN_TABLE }

SELECT 'check_no_if_exists_missing_db';
SYSTEM SYNC REPLICA missing_db_04141.missing_table_04141; -- { serverError UNKNOWN_DATABASE }

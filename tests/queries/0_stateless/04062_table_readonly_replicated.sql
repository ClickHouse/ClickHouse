-- Tags: zookeeper

-- The `table_readonly` setting is not supported for ReplicatedMergeTree.

-- CREATE TABLE should fail.
CREATE TABLE t_readonly_repl (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readonly_repl', 'r1') ORDER BY x SETTINGS table_readonly = 1; -- { serverError NOT_IMPLEMENTED }

-- ATTACH TABLE should fail too.
-- The UUID-form is required because `Atomic` rejects `ATTACH TABLE name (cols) ENGINE = ...` syntax before reaching the storage check.
ATTACH TABLE t_readonly_repl UUID 'aaaaaaaa-1111-2222-3333-aaaaaaaaaaaa' (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readonly_repl', 'r1') ORDER BY x SETTINGS table_readonly = 1; -- { serverError NOT_IMPLEMENTED }

-- ALTER MODIFY SETTING should fail.
CREATE TABLE t_readonly_repl (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readonly_repl', 'r1') ORDER BY x;
ALTER TABLE t_readonly_repl MODIFY SETTING table_readonly = 1; -- { serverError NOT_IMPLEMENTED }

-- A mixed ALTER (column + settings) must not slip the setting through the non-pure-settings path.
ALTER TABLE t_readonly_repl ADD COLUMN y UInt64, MODIFY SETTING table_readonly = 1; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_readonly_repl;

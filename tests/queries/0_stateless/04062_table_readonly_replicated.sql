-- Tags: zookeeper

-- The `table_readonly` setting is not supported for ReplicatedMergeTree.

-- CREATE TABLE should fail.
CREATE TABLE t_readonly_repl (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readonly_repl', 'r1') ORDER BY x SETTINGS table_readonly = 1; -- { serverError NOT_IMPLEMENTED }

-- ATTACH TABLE should fail too.
ATTACH TABLE t_readonly_repl (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readonly_repl', 'r1') ORDER BY x SETTINGS table_readonly = 1; -- { serverError NOT_IMPLEMENTED }

-- ALTER MODIFY SETTING should fail.
CREATE TABLE t_readonly_repl (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readonly_repl', 'r1') ORDER BY x;
ALTER TABLE t_readonly_repl MODIFY SETTING table_readonly = 1; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_readonly_repl;

-- Tags: no-replicated-database
-- Tag no-replicated-database: Unsupported type of ALTER query

DROP TABLE IF EXISTS t;

# Memory, MergeTree, and ReplicatedMergeTree

CREATE TABLE t (x UInt8) ENGINE = Memory COMMENT 'Hello';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
ALTER TABLE t MODIFY COMMENT 'World';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
DROP TABLE t;

CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY () COMMENT 'Hello';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
ALTER TABLE t MODIFY COMMENT 'World';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
DROP TABLE t;

# The case when there are many operations in one ALTER

CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY () COMMENT 'Hello';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
ALTER TABLE t MODIFY COMMENT 'World', MODIFY COLUMN x UInt16;
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
DROP TABLE t;

# Note that the table comment is not replicated. We can implement it later.

CREATE TABLE t (x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_comment_table1/t', '1') ORDER BY () COMMENT 'Hello';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
ALTER TABLE t MODIFY COMMENT 'World';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
DROP TABLE t SYNC;

CREATE TABLE t (x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_comment_table2/t', '1') ORDER BY () COMMENT 'Hello';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
ALTER TABLE t MODIFY COMMENT 'World', MODIFY COLUMN x UInt16;
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
DROP TABLE t SYNC;

# The cases when there is no comment on creation

CREATE TABLE t (x UInt8) ENGINE = Memory;
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
ALTER TABLE t MODIFY COMMENT 'World';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
DROP TABLE t;

CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY ();
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
ALTER TABLE t MODIFY COMMENT 'World';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
DROP TABLE t;

CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY ();
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
ALTER TABLE t MODIFY COMMENT 'World', MODIFY COLUMN x UInt16;
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
DROP TABLE t;

CREATE TABLE t (x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_comment_table3/t', '1') ORDER BY ();
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
ALTER TABLE t MODIFY COMMENT 'World';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
DROP TABLE t SYNC;

CREATE TABLE t (x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_comment_table4/t', '1') ORDER BY ();
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
ALTER TABLE t MODIFY COMMENT 'World', MODIFY COLUMN x UInt16;
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't';
DROP TABLE t SYNC;

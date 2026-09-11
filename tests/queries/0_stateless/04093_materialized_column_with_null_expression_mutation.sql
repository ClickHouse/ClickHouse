-- Tags: memory-engine, system-zookeeper
--
-- Test that mutation commands on tables containing MATERIALIZED columns
-- (e.g. from system.zookeeper) do not crash.
-- system.zookeeper marks read-only columns as MATERIALIZED with a constant expression
-- to block INSERT. MutationsInterpreter must handle null expression defensively.

SET allow_experimental_alias_table_engine = 1;
SET enable_analyzer = 1;

-- Case 1: UPDATE a MATERIALIZED column should be rejected with CANNOT_UPDATE_COLUMN
CREATE TABLE t0 ENGINE = Alias(system, zookeeper);
CREATE TABLE t1 AS t0 ENGINE = Memory();
ALTER TABLE t1 UPDATE czxid = 1 WHERE TRUE; -- {serverError CANNOT_UPDATE_COLUMN}
DROP TABLE t1;
DROP TABLE t0;

-- Case 2: Same with CREATE TABLE t1 AS system.zookeeper
CREATE TABLE t1 AS system.zookeeper ENGINE = Memory();
ALTER TABLE t1 UPDATE czxid = 1 WHERE TRUE; -- {serverError CANNOT_UPDATE_COLUMN}
DROP TABLE t1;

-- Case 3: CLEAR COLUMN on a MATERIALIZED column
CREATE TABLE t1 AS system.zookeeper ENGINE = Memory();
INSERT INTO t1 (name, value, path, zookeeperName) VALUES ('test', 'val', '/', 'default');
ALTER TABLE t1 CLEAR COLUMN czxid IN PARTITION tuple();
SELECT count() FROM t1;
DROP TABLE t1;

-- Case 4: CLEAR COLUMN on a regular column in a table with MATERIALIZED columns.
-- This exercises the dependency check loop and the recalculation step in MutationsInterpreter.
CREATE TABLE t1 AS system.zookeeper ENGINE = Memory();
INSERT INTO t1 (name, value, path, zookeeperName) VALUES ('test', 'val', '/', 'default');
ALTER TABLE t1 CLEAR COLUMN name IN PARTITION tuple();
SELECT count() FROM t1;
DROP TABLE t1;

-- Case 5: Verify that MATERIALIZED columns in system.zookeeper have a non-empty default_expression.
-- Without our fix, expression would be null, causing null pointer dereference in MutationsInterpreter
-- and MATERIALIZED attribute loss during DatabaseReplicated DDL serialization.
SELECT count() > 0 FROM system.columns
WHERE database = 'system' AND table = 'zookeeper'
  AND default_kind = 'MATERIALIZED' AND default_expression != '';

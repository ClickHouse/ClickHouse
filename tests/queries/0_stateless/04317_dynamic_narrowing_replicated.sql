-- Tags: no-fasttest, zookeeper
-- Test: Dynamic column narrowing with ReplicatedMergeTree
-- Verifies that narrowed parts are correctly replicated between replicas

DROP TABLE IF EXISTS t_narrow_r1;
DROP TABLE IF EXISTS t_narrow_r2;

CREATE TABLE t_narrow_r1 (id UInt64, value Dynamic)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_narrow_repl', 'r1')
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

CREATE TABLE t_narrow_r2 (id UInt64, value Dynamic)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_narrow_repl', 'r2')
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

-- Insert into replica 1 (homogeneous Int64 — triggers narrowing)
INSERT INTO t_narrow_r1 SELECT number, number::Int64 FROM numbers(500);

-- Wait for replication
SYSTEM SYNC REPLICA t_narrow_r2;

SELECT '-- 1. replica 1';
SELECT count(), min(value::Int64), max(value::Int64) FROM t_narrow_r1;
SELECT '-- 1. replica 2 (replicated)';
SELECT count(), min(value::Int64), max(value::Int64) FROM t_narrow_r2;

-- Insert into replica 2 (homogeneous String)
INSERT INTO t_narrow_r2 SELECT number + 500, 'val_' || toString(number) FROM numbers(500);

SYSTEM SYNC REPLICA t_narrow_r1;

SELECT '-- 2. both replicas after cross-insert';
SELECT count() FROM t_narrow_r1;
SELECT count() FROM t_narrow_r2;
SELECT dynamicType(value) AS t, count() AS c FROM t_narrow_r1 GROUP BY t ORDER BY t;

-- Merge on replica 1
OPTIMIZE TABLE t_narrow_r1 FINAL;
SYSTEM SYNC REPLICA t_narrow_r2;

SELECT '-- 3. after merge + sync';
SELECT count() FROM t_narrow_r1;
SELECT count() FROM t_narrow_r2;
SELECT dynamicType(value) AS t, count() AS c FROM t_narrow_r2 GROUP BY t ORDER BY t;

-- Verify actual values from replica 2
SELECT value::Int64 FROM t_narrow_r2 WHERE dynamicType(value) = 'Int64' ORDER BY value::Int64 LIMIT 3;
SELECT value::String FROM t_narrow_r2 WHERE dynamicType(value) = 'String' ORDER BY value::String LIMIT 3;

DROP TABLE IF EXISTS t_narrow_r1;
DROP TABLE IF EXISTS t_narrow_r2;

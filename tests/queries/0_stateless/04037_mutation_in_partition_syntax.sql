-- Tags: no-replicated-database, no-parallel
-- Test parsing and error handling for multi-partition IN PARTITION syntax.

SET mutations_sync = 1;

-- ============================================================
-- Positive parser tests: complex partition expressions
-- ============================================================

-- Test 1: Tuple partition key with multi-partition IN PARTITION
DROP TABLE IF EXISTS t_parse_tuple;
CREATE TABLE t_parse_tuple (key UInt64, value String, a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parse_tuple', '1')
PARTITION BY (a, b) ORDER BY key;

INSERT INTO t_parse_tuple VALUES (1, 'x', 1, 10), (2, 'y', 1, 20), (3, 'z', 2, 10);

ALTER TABLE t_parse_tuple DELETE IN PARTITION (1, 10), (2, 10) WHERE 1;

SELECT 'test 1: tuple partition multi-delete';
SELECT * FROM t_parse_tuple ORDER BY key;

DROP TABLE t_parse_tuple SYNC;

-- Test 2: Three partitions in a single statement
DROP TABLE IF EXISTS t_parse_three;
CREATE TABLE t_parse_three (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parse_three', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_parse_three VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-02'), (3, 'c', '2024-01-03'), (4, 'd', '2024-01-04');

ALTER TABLE t_parse_three DELETE IN PARTITION '2024-01-01', '2024-01-02', '2024-01-03' WHERE 1;

SELECT 'test 2: three partitions';
SELECT * FROM t_parse_three ORDER BY key;

DROP TABLE t_parse_three SYNC;

-- Test 3: Mix of PARTITION ID and tuple syntax in multi-partition
DROP TABLE IF EXISTS t_parse_mixed_id;
CREATE TABLE t_parse_mixed_id (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parse_mixed_id', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_parse_mixed_id VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-02'), (3, 'c', '2024-01-03');

ALTER TABLE t_parse_mixed_id DELETE IN PARTITION ID '20240101', ID '20240103' WHERE 1;

SELECT 'test 3: mixed partition ID';
SELECT * FROM t_parse_mixed_id ORDER BY key;

DROP TABLE t_parse_mixed_id SYNC;

-- Test 4: UPDATE with tuple partition and multiple partitions
DROP TABLE IF EXISTS t_parse_update_tuple;
CREATE TABLE t_parse_update_tuple (key UInt64, value String, a UInt32, b String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parse_update_tuple', '1')
PARTITION BY (a, b) ORDER BY key;

INSERT INTO t_parse_update_tuple VALUES (1, 'old', 1, 'x'), (2, 'old', 1, 'y'), (3, 'old', 2, 'x');

ALTER TABLE t_parse_update_tuple UPDATE value = 'new' IN PARTITION (1, 'x'), (2, 'x') WHERE 1;

SELECT 'test 4: update with tuple partition';
SELECT * FROM t_parse_update_tuple ORDER BY key;

DROP TABLE t_parse_update_tuple SYNC;

-- Test 5: Single partition (backward compatibility with original syntax)
DROP TABLE IF EXISTS t_parse_single;
CREATE TABLE t_parse_single (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parse_single', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_parse_single VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-02');

ALTER TABLE t_parse_single DELETE IN PARTITION '2024-01-01' WHERE 1;

SELECT 'test 5: single partition (backward compat)';
SELECT * FROM t_parse_single ORDER BY key;

DROP TABLE t_parse_single SYNC;

-- Test 6: Lightweight DELETE with multi-partition IN PARTITION
DROP TABLE IF EXISTS t_parse_lw_delete;
CREATE TABLE t_parse_lw_delete (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parse_lw_delete', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_parse_lw_delete VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-02'), (3, 'c', '2024-01-03');

DELETE FROM t_parse_lw_delete IN PARTITION '2024-01-01', '2024-01-03' WHERE 1;

SELECT 'test 6: lightweight delete multi-partition';
SELECT * FROM t_parse_lw_delete ORDER BY key;

DROP TABLE t_parse_lw_delete SYNC;

-- ============================================================
-- Negative tests: syntax errors and invalid partitions
-- ============================================================

-- Test 7: Missing WHERE clause after multi-partition
ALTER TABLE system.one DELETE IN PARTITION '2024-01-01', '2024-01-02'; -- { clientError SYNTAX_ERROR }

-- Test 8: Non-existing partition - should succeed but affect no rows
DROP TABLE IF EXISTS t_parse_neg_nonexist;
CREATE TABLE t_parse_neg_nonexist (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parse_neg_nonexist', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_parse_neg_nonexist VALUES (1, 'a', '2024-01-01');

ALTER TABLE t_parse_neg_nonexist DELETE IN PARTITION '2099-12-31' WHERE 1;

SELECT 'test 8: non-existing partition';
SELECT * FROM t_parse_neg_nonexist ORDER BY key;

DROP TABLE t_parse_neg_nonexist SYNC;

-- Test 9: Wrong partition value type for tuple key
DROP TABLE IF EXISTS t_parse_neg_wrong_type;
CREATE TABLE t_parse_neg_wrong_type (key UInt64, a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parse_neg_wrong_type', '1')
PARTITION BY (a, b) ORDER BY key;

INSERT INTO t_parse_neg_wrong_type VALUES (1, 1, 10);

ALTER TABLE t_parse_neg_wrong_type DELETE IN PARTITION 'not_a_tuple' WHERE 1; -- { serverError INVALID_PARTITION_VALUE }

DROP TABLE t_parse_neg_wrong_type SYNC;

-- Test 10: Multi-partition where some partitions don't exist yet in ZK
DROP TABLE IF EXISTS t_parse_new_parts;
CREATE TABLE t_parse_new_parts (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parse_new_parts', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_parse_new_parts VALUES (1, 'a', '2024-01-01');

-- '2024-01-02' partition doesn't exist yet, should be created automatically
ALTER TABLE t_parse_new_parts DELETE IN PARTITION '2024-01-01', '2024-01-02' WHERE 1;

-- Insert into the newly created partition to verify it works
INSERT INTO t_parse_new_parts VALUES (2, 'b', '2024-01-01'), (3, 'c', '2024-01-02');

SELECT 'test 10: multi-partition with new partition';
SELECT * FROM t_parse_new_parts ORDER BY key;

DROP TABLE t_parse_new_parts SYNC;

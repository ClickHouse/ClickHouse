-- Tags: no-replicated-database
-- Test that mutations work correctly with the new analyzer path.

SET allow_experimental_analyzer = 1;

-- Test 1: MergeTree engine - DELETE
DROP TABLE IF EXISTS t_mutations_analyzer;
CREATE TABLE t_mutations_analyzer (id UInt64, value String, amount Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mutations_analyzer VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50);

ALTER TABLE t_mutations_analyzer DELETE WHERE id IN (2, 4) SETTINGS mutations_sync = 2;
SELECT * FROM t_mutations_analyzer ORDER BY id;

-- Test 2: MergeTree engine - UPDATE
ALTER TABLE t_mutations_analyzer UPDATE value = 'updated', amount = amount * 2 WHERE id >= 3 SETTINGS mutations_sync = 2;
SELECT * FROM t_mutations_analyzer ORDER BY id;

-- Test 3: MergeTree engine - DELETE with subquery (tests prepared sets)
DROP TABLE IF EXISTS t_mutations_analyzer_filter;
CREATE TABLE t_mutations_analyzer_filter (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mutations_analyzer_filter VALUES (1), (5);

ALTER TABLE t_mutations_analyzer DELETE WHERE id IN (SELECT id FROM t_mutations_analyzer_filter) SETTINGS mutations_sync = 2;
SELECT * FROM t_mutations_analyzer ORDER BY id;

-- Test 4: MergeTree engine - MATERIALIZE COLUMN
DROP TABLE IF EXISTS t_mutations_analyzer_mat;
CREATE TABLE t_mutations_analyzer_mat (id UInt64, value String DEFAULT 'default_val') ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mutations_analyzer_mat (id) VALUES (1), (2), (3);

ALTER TABLE t_mutations_analyzer_mat MATERIALIZE COLUMN value SETTINGS mutations_sync = 2;
SELECT * FROM t_mutations_analyzer_mat ORDER BY id;

-- Test 5: Memory engine - DELETE (non-MergeTree Source::read path)
DROP TABLE IF EXISTS t_mutations_analyzer_mem;
CREATE TABLE t_mutations_analyzer_mem (id UInt64, value String) ENGINE = Memory;
INSERT INTO t_mutations_analyzer_mem VALUES (1, 'x'), (2, 'y'), (3, 'z');

ALTER TABLE t_mutations_analyzer_mem DELETE WHERE id = 2 SETTINGS mutations_sync = 2;
SELECT * FROM t_mutations_analyzer_mem ORDER BY id;

-- Test 6: Memory engine - UPDATE
ALTER TABLE t_mutations_analyzer_mem UPDATE value = 'modified' WHERE id = 3 SETTINGS mutations_sync = 2;
SELECT * FROM t_mutations_analyzer_mem ORDER BY id;

-- Cleanup
DROP TABLE t_mutations_analyzer;
DROP TABLE t_mutations_analyzer_filter;
DROP TABLE t_mutations_analyzer_mat;
DROP TABLE t_mutations_analyzer_mem;

-- Tags: no-shared-merge-tree
-- SMT does not support DETACH/ATTACH PARTITION

-- Test that ALTER TABLE operations work correctly with Bool partition keys.
-- Bool partition values must produce the same partition ID whether they come
-- from the INSERT path (UInt64 Field) or the ALTER query path (was Bool Field).
-- See https://github.com/ClickHouse/ClickHouse/issues/101722

DROP TABLE IF EXISTS t_bool_part;
CREATE TABLE t_bool_part (c0 Bool, val UInt32) ENGINE = MergeTree() ORDER BY val PARTITION BY c0;
INSERT INTO t_bool_part VALUES (false, 1), (false, 2), (true, 3), (true, 4);

SELECT 'drop partition 1';
ALTER TABLE t_bool_part DROP PARTITION 1;
SELECT c0, val FROM t_bool_part ORDER BY val;

-- Re-insert dropped rows
INSERT INTO t_bool_part VALUES (true, 3), (true, 4);

SELECT 'drop partition true';
ALTER TABLE t_bool_part DROP PARTITION true;
SELECT c0, val FROM t_bool_part ORDER BY val;

-- Re-insert
INSERT INTO t_bool_part VALUES (true, 3), (true, 4);

SELECT 'drop partition 0';
ALTER TABLE t_bool_part DROP PARTITION 0;
SELECT c0, val FROM t_bool_part ORDER BY val;

-- Re-insert
INSERT INTO t_bool_part VALUES (false, 1), (false, 2);

SELECT 'drop partition false';
ALTER TABLE t_bool_part DROP PARTITION false;
SELECT c0, val FROM t_bool_part ORDER BY val;

DROP TABLE t_bool_part;

-- Test DETACH/ATTACH with Bool partition key
DROP TABLE IF EXISTS t_bool_detach;
CREATE TABLE t_bool_detach (c0 Bool, val UInt32) ENGINE = MergeTree() ORDER BY val PARTITION BY c0;
INSERT INTO t_bool_detach VALUES (false, 1), (true, 2);

SELECT 'detach partition 1';
ALTER TABLE t_bool_detach DETACH PARTITION 1;
SELECT c0, val FROM t_bool_detach ORDER BY val;

SELECT 'attach partition 1';
ALTER TABLE t_bool_detach ATTACH PARTITION 1;
SELECT c0, val FROM t_bool_detach ORDER BY val;

DROP TABLE t_bool_detach;

-- Test multi-column partition key containing Bool
DROP TABLE IF EXISTS t_bool_multi;
CREATE TABLE t_bool_multi (a Bool, b UInt32, val String) ENGINE = MergeTree() ORDER BY val PARTITION BY (a, b);
INSERT INTO t_bool_multi VALUES (false, 1, 'a'), (true, 2, 'b'), (true, 1, 'c');

SELECT 'drop partition (1, 2)';
ALTER TABLE t_bool_multi DROP PARTITION (1, 2);
SELECT a, b, val FROM t_bool_multi ORDER BY val;

DROP TABLE t_bool_multi;

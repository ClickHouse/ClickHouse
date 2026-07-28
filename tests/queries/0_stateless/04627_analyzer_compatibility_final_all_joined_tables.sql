-- Compatibility setting for the fix of FINAL leaking onto other tables of a JOIN
-- (https://github.com/ClickHouse/ClickHouse/pull/108979).

-- The setting `analyzer_compatibility_apply_final_to_all_joined_tables` has an effect only when
-- the analyzer is enabled. The old analyzer has different semantics of FINAL in JOIN (it ignores
-- FINAL on the right table), so pin the analyzer explicitly.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (id Int64, right_id Int64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY id;
CREATE TABLE t_right (id Int64, attr String, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY id;

SYSTEM STOP MERGES t_left;
SYSTEM STOP MERGES t_right;

-- One logical row per table, two unmerged versions each (separate parts).
INSERT INTO t_left VALUES (1, 10, 1);
INSERT INTO t_left VALUES (1, 10, 2);
INSERT INTO t_right VALUES (10, 'car', 1);
INSERT INTO t_right VALUES (10, 'car', 2);

SELECT 'FINAL on the left table only, default behavior';
SELECT count() FROM t_left AS l FINAL INNER JOIN t_right AS r ON r.id = l.right_id;

SELECT 'FINAL on the left table only, compatibility setting enabled';
SELECT count() FROM t_left AS l FINAL INNER JOIN t_right AS r ON r.id = l.right_id
SETTINGS analyzer_compatibility_apply_final_to_all_joined_tables = 1;

SELECT 'FINAL on the left table only, compatibility with an older version';
SELECT count() FROM t_left AS l FINAL INNER JOIN t_right AS r ON r.id = l.right_id
SETTINGS compatibility = '26.5';

SELECT 'FINAL on both tables is unaffected by the setting';
SELECT count() FROM t_left AS l FINAL INNER JOIN t_right AS r FINAL ON r.id = l.right_id;
SELECT count() FROM t_left AS l FINAL INNER JOIN t_right AS r FINAL ON r.id = l.right_id
SETTINGS analyzer_compatibility_apply_final_to_all_joined_tables = 1;

SELECT 'FINAL on the right table only does not leak to the left table even with the setting';
SELECT count() FROM t_left AS l INNER JOIN t_right AS r FINAL ON r.id = l.right_id;
SELECT count() FROM t_left AS l INNER JOIN t_right AS r FINAL ON r.id = l.right_id
SETTINGS analyzer_compatibility_apply_final_to_all_joined_tables = 1;

DROP TABLE t_left;
DROP TABLE t_right;

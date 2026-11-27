-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/99959
-- In a LEFT ANTI JOIN, the result contains only rows from the left table that have no
-- matching row in the right table. The right-table columns (including the join key)
-- must contain default values for those unmatched rows, not the left key value.

DROP TABLE IF EXISTS left_99959;
DROP TABLE IF EXISTS right_99959;
DROP TABLE IF EXISTS right_99959_multi;
DROP TABLE IF EXISTS left_99959_str;
DROP TABLE IF EXISTS right_99959_str;
DROP TABLE IF EXISTS left_99959_multi_keys;
DROP TABLE IF EXISTS right_99959_multi_keys;

CREATE TABLE left_99959 (x Int32) ENGINE = Memory;
CREATE TABLE right_99959 (x Int32) ENGINE = Memory;
INSERT INTO left_99959 VALUES (1), (2), (3);
INSERT INTO right_99959 VALUES (2);

SELECT '--- LEFT ANTI JOIN, ON syntax ---';
SELECT a.x AS left_x, b.x AS right_x
FROM left_99959 AS a
LEFT ANTI JOIN right_99959 AS b ON (a.x = b.x)
ORDER BY left_x;

SELECT '--- LEFT ANTI JOIN, USING syntax ---';
SELECT a.x AS left_x, b.x AS right_x
FROM left_99959 AS a
LEFT ANTI JOIN right_99959 AS b USING (x)
ORDER BY left_x;

SELECT '--- LEFT ANTI JOIN, parallel_hash algorithm ---';
SELECT a.x AS left_x, b.x AS right_x
FROM left_99959 AS a
LEFT ANTI JOIN right_99959 AS b ON (a.x = b.x)
ORDER BY left_x
SETTINGS join_algorithm = 'parallel_hash';

SELECT '--- LEFT ANTI JOIN, grace_hash algorithm ---';
SELECT a.x AS left_x, b.x AS right_x
FROM left_99959 AS a
LEFT ANTI JOIN right_99959 AS b ON (a.x = b.x)
ORDER BY left_x
SETTINGS join_algorithm = 'grace_hash';

SELECT '--- LEFT ANTI JOIN with multiple right columns: only the key was affected, other defaults must keep working ---';
CREATE TABLE right_99959_multi (x Int32, y String, z Nullable(Int64)) ENGINE = Memory;
INSERT INTO right_99959_multi VALUES (2, 'two', 200);
SELECT a.x AS left_x, b.x AS right_x, b.y AS right_y, b.z AS right_z
FROM left_99959 AS a
LEFT ANTI JOIN right_99959_multi AS b ON (a.x = b.x)
ORDER BY left_x;

SELECT '--- LEFT ANTI JOIN, String keys ---';
CREATE TABLE left_99959_str (s String) ENGINE = Memory;
CREATE TABLE right_99959_str (s String) ENGINE = Memory;
INSERT INTO left_99959_str VALUES ('alpha'), ('beta'), ('gamma');
INSERT INTO right_99959_str VALUES ('beta');
SELECT a.s AS left_s, b.s AS right_s
FROM left_99959_str AS a
LEFT ANTI JOIN right_99959_str AS b ON (a.s = b.s)
ORDER BY left_s;

SELECT '--- RIGHT ANTI JOIN with table swap (becomes LEFT ANTI internally) ---';
SELECT c.x AS left_x, d.x AS right_x
FROM right_99959 AS c
RIGHT ANTI JOIN left_99959 AS d ON (c.x = d.x)
ORDER BY right_x
SETTINGS query_plan_join_swap_table = 'true';

SELECT '--- RIGHT ANTI JOIN without table swap ---';
SELECT c.x AS left_x, d.x AS right_x
FROM right_99959 AS c
RIGHT ANTI JOIN left_99959 AS d ON (c.x = d.x)
ORDER BY right_x
SETTINGS query_plan_join_swap_table = 'false';

SELECT '--- LEFT ANTI JOIN, multiple join keys ---';
CREATE TABLE left_99959_multi_keys (x Int32, y Int32) ENGINE = Memory;
CREATE TABLE right_99959_multi_keys (x Int32, y Int32) ENGINE = Memory;
INSERT INTO left_99959_multi_keys VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO right_99959_multi_keys VALUES (2, 20);
SELECT a.x AS left_x, a.y AS left_y, b.x AS right_x, b.y AS right_y
FROM left_99959_multi_keys AS a
LEFT ANTI JOIN right_99959_multi_keys AS b ON (a.x = b.x AND a.y = b.y)
ORDER BY left_x;

DROP TABLE left_99959;
DROP TABLE right_99959;
DROP TABLE right_99959_multi;
DROP TABLE left_99959_str;
DROP TABLE right_99959_str;
DROP TABLE left_99959_multi_keys;
DROP TABLE right_99959_multi_keys;

-- Tests that queries against a table with the `Join` engine return correct values when they read
-- only some of the table's columns: a later column, a reordered subset, or a single column via
-- `joinGet`. A `Join` engine table stores all of its columns even when a query reads only some of
-- them, so the server has to pick the right stored column for each requested one; an ordinary join
-- reads exactly the columns the query needs, which is why this cannot be tested without the `Join`
-- engine. The non-key columns have distinct types and values, so reading a wrong column produces a
-- visibly wrong result (or a type error) rather than plausible-looking data.

DROP TABLE IF EXISTS t_join_any;
DROP TABLE IF EXISTS t_join_all;

CREATE TABLE t_join_any (k UInt32, a UInt64, b String, c Int32) ENGINE = Join(ANY, LEFT, k);
INSERT INTO t_join_any VALUES (1, 1100, 'bee1', -11), (2, 2200, 'bee2', -22), (3, 3300, 'bee3', -33);

-- Expect the `b` value of each key ('bee1'..'bee3'). Key 4 has no match: expect the empty string
-- (the LEFT JOIN default for `String`).
SELECT 'ANY LEFT JOIN, select only the second non-key column (b)';
SELECT l.k, j.b FROM (SELECT toUInt32(arrayJoin([1, 2, 3, 4])) AS k) AS l ANY LEFT JOIN t_join_any AS j ON l.k = j.k ORDER BY l.k;

-- Expect the `c` value of each key (-11, -22, -33), and 0 (the `Int32` default) for the
-- unmatched key 4.
SELECT 'ANY LEFT JOIN, select only the third non-key column (c)';
SELECT l.k, j.c FROM (SELECT toUInt32(arrayJoin([1, 2, 3, 4])) AS k) AS l ANY LEFT JOIN t_join_any AS j ON l.k = j.k ORDER BY l.k;

-- Expect `c` and `b` of each key, in that (reversed) order.
SELECT 'ANY LEFT JOIN, select a reordered subset (c, b)';
SELECT l.k, j.c, j.b FROM (SELECT toUInt32(arrayJoin([1, 2, 3])) AS k) AS l ANY LEFT JOIN t_join_any AS j ON l.k = j.k ORDER BY l.k;

-- Expect the `b` value of key 2 ('bee2') and the `c` value of key 3 (-33).
SELECT 'joinGet of the non-first columns';
SELECT joinGet(currentDatabase() || '.t_join_any', 'b', toUInt32(2));
SELECT joinGet(currentDatabase() || '.t_join_any', 'c', toUInt32(3));

CREATE TABLE t_join_all (k UInt32, a UInt64, b String) ENGINE = Join(ALL, INNER, k);
INSERT INTO t_join_all VALUES (1, 10, 'x1'), (1, 20, 'x2'), (2, 30, 'y1');

-- ALL mode keeps duplicate keys: expect both `b` values of key 1 ('x1' and 'x2') and the single
-- `b` value of key 2 ('y1').
SELECT 'ALL INNER JOIN with duplicate keys, select only the second non-key column (b)';
SELECT l.k, j.b FROM (SELECT toUInt32(arrayJoin([1, 2])) AS k) AS l ALL INNER JOIN t_join_all AS j ON l.k = j.k ORDER BY l.k, j.b;

DROP TABLE t_join_any;
DROP TABLE t_join_all;

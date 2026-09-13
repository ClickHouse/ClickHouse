-- A view whose body ends with a `LIMIT [n] AFTER/UNTIL` range selects rows by their position in the
-- view's own stream, so an outer `ORDER BY ... LIMIT` must not be pushed into the view: the injected
-- `ORDER BY` would change which rows the boundaries select, and the injected count would cap a range
-- that had none. The rows are inserted as one block into a `Memory` table, so the view reads them in
-- insertion order and the expected results are fixed.
SET max_threads = 1;

DROP TABLE IF EXISTS t_view_range_src;
DROP VIEW IF EXISTS v_range_unbounded;
DROP VIEW IF EXISTS v_range_counted;
DROP VIEW IF EXISTS v_range_until;

CREATE TABLE t_view_range_src (x UInt64) ENGINE = Memory;
INSERT INTO t_view_range_src SELECT number FROM numbers(10);

CREATE VIEW v_range_unbounded AS SELECT x FROM t_view_range_src LIMIT AFTER x = 5 UNTIL x = 8;
CREATE VIEW v_range_counted AS SELECT x FROM t_view_range_src LIMIT 3 AFTER x >= 4;
CREATE VIEW v_range_until AS SELECT x FROM t_view_range_src LIMIT UNTIL x = 3;

-- The views expose [5, 6, 7], [4, 5, 6] and [0, 1, 2] respectively; the outer query takes the two
-- largest values of each.
SELECT groupArray(x) FROM (SELECT x FROM v_range_unbounded ORDER BY x DESC LIMIT 2) SETTINGS enable_analyzer = 1;
SELECT groupArray(x) FROM (SELECT x FROM v_range_counted ORDER BY x DESC LIMIT 2) SETTINGS enable_analyzer = 1;
SELECT groupArray(x) FROM (SELECT x FROM v_range_until ORDER BY x DESC LIMIT 2) SETTINGS enable_analyzer = 1;

SELECT groupArray(x) FROM (SELECT x FROM v_range_unbounded ORDER BY x DESC LIMIT 2) SETTINGS enable_analyzer = 0;
SELECT groupArray(x) FROM (SELECT x FROM v_range_counted ORDER BY x DESC LIMIT 2) SETTINGS enable_analyzer = 0;
SELECT groupArray(x) FROM (SELECT x FROM v_range_until ORDER BY x DESC LIMIT 2) SETTINGS enable_analyzer = 0;

DROP VIEW v_range_unbounded;
DROP VIEW v_range_counted;
DROP VIEW v_range_until;
DROP TABLE t_view_range_src;

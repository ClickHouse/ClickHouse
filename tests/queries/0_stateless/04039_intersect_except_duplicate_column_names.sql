-- Reproducer for heap-use-after-free in IntersectOrExceptTransform
-- when the header has duplicate column names (e.g., from SELECT col, *, col).
-- The bug was that getPositionByName returned the same position for duplicate names,
-- creating duplicate entries in key_columns_pos. Then convertToFullColumnIfConst
-- on the same position freed the column a raw pointer still referenced.

DROP TABLE IF EXISTS t_intersect_except;
CREATE TABLE t_intersect_except (id UInt32, a String, b String) ENGINE = Memory;
INSERT INTO t_intersect_except VALUES (1, 'hello', 'world'), (2, 'foo', 'bar');

-- SELECT id, *, b produces duplicate column names: id appears twice, b appears twice.
(SELECT id, *, b FROM t_intersect_except ORDER BY id LIMIT 10) EXCEPT DISTINCT (SELECT id, *, b FROM t_intersect_except ORDER BY id LIMIT 10);

(SELECT id, *, b FROM t_intersect_except ORDER BY id LIMIT 10) INTERSECT DISTINCT (SELECT id, *, b FROM t_intersect_except ORDER BY id LIMIT 10);

(SELECT id, *, b FROM t_intersect_except ORDER BY id LIMIT 10) EXCEPT ALL (SELECT id, *, b FROM t_intersect_except ORDER BY id LIMIT 10);

(SELECT id, *, b FROM t_intersect_except ORDER BY id LIMIT 10) INTERSECT ALL (SELECT id, *, b FROM t_intersect_except ORDER BY id LIMIT 10);

DROP TABLE t_intersect_except;

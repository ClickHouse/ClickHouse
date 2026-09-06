-- https://github.com/ClickHouse/ClickHouse/issues/112035
-- `indexHint` carries its condition in a separate inner expression, so in the outer one it reads no
-- column and was pushed to both sides of the join. On the non-preserved side of an OUTER join that is
-- not a no-op: pruning granules there turns matched rows into unmatched ones, the preserved side then
-- gets default values for the right columns, and those defaults pass the accompanying predicate.

DROP TABLE IF EXISTS t_hint_join_left;
DROP TABLE IF EXISTS t_hint_join_right;
CREATE TABLE t_hint_join_left (id UInt32) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
CREATE TABLE t_hint_join_right (id UInt32, v Int64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
INSERT INTO t_hint_join_left SELECT number FROM numbers(1000);
INSERT INTO t_hint_join_right SELECT number, number + 1000 FROM numbers(1000);

SELECT count() FROM t_hint_join_left AS l LEFT JOIN t_hint_join_right AS r ON l.id = r.id
WHERE indexHint(r.id < 150) AND (r.id < 150);
SELECT count() FROM t_hint_join_left AS l LEFT JOIN t_hint_join_right AS r ON l.id = r.id
WHERE r.id < 150;
SELECT count() FROM t_hint_join_left AS l INNER JOIN t_hint_join_right AS r ON l.id = r.id
WHERE indexHint(r.id < 150) AND (r.id < 150);
SELECT count() FROM t_hint_join_left AS l LEFT JOIN t_hint_join_right AS r ON l.id = r.id
WHERE indexHint(l.id < 150) AND (l.id < 150);
SELECT count() FROM t_hint_join_left AS l RIGHT JOIN t_hint_join_right AS r ON l.id = r.id
WHERE indexHint(l.id < 150) AND (l.id < 150);
SELECT count() FROM t_hint_join_left AS l RIGHT JOIN t_hint_join_right AS r ON l.id = r.id
WHERE l.id < 150;
SELECT count() FROM t_hint_join_left AS l FULL JOIN t_hint_join_right AS r ON l.id = r.id
WHERE indexHint(r.id < 150) AND (r.id < 150);
SELECT count() FROM t_hint_join_left AS l FULL JOIN t_hint_join_right AS r ON l.id = r.id
WHERE r.id < 150;

SELECT 'every surviving row is a real match';
SELECT isNull(r.v) AS unmatched, count() FROM t_hint_join_left AS l LEFT JOIN t_hint_join_right AS r ON l.id = r.id
WHERE indexHint(r.id < 150) AND (r.id < 150) GROUP BY unmatched ORDER BY unmatched
SETTINGS join_use_nulls = 1;

SELECT 'a hint over a single table still narrows the read';
SELECT count() FROM t_hint_join_right WHERE indexHint(id < 150) AND (id < 150);
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT id FROM t_hint_join_right WHERE indexHint(id < 150))
WHERE explain LIKE '%Granules: 3/16%';

DROP TABLE t_hint_join_left;
DROP TABLE t_hint_join_right;

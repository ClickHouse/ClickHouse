-- A WHERE condition that is not boolean must keep the semantics of `and` when the other conjuncts
-- are merged into the JOIN condition: every non-zero value is true, not only the ones that survive
-- a cast to UInt8.

DROP TABLE IF EXISTS t_merge_filter;

CREATE TABLE t_merge_filter (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_merge_filter SELECT number FROM numbers(600);

SELECT count() FROM t_merge_filter AS l LEFT JOIN t_merge_filter AS r ON l.id = r.id
WHERE r.id AND l.id = r.id
SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT count() FROM t_merge_filter AS l LEFT JOIN t_merge_filter AS r ON l.id = r.id
WHERE r.id AND l.id = r.id
SETTINGS query_plan_merge_filter_into_join_condition = 0;

SELECT l.id FROM t_merge_filter AS l LEFT JOIN t_merge_filter AS r ON l.id = r.id
WHERE r.id AND l.id = r.id AND l.id IN (0, 255, 256, 512)
ORDER BY l.id;

-- The condition is also a part of the result, so it has to be converted to the original type.
SELECT l.id, r.id / 256 AND l.id = r.id AS c FROM t_merge_filter AS l LEFT JOIN t_merge_filter AS r ON l.id = r.id
WHERE r.id / 256 AND l.id = r.id AND l.id IN (0, 255, 256, 512)
ORDER BY l.id;

DROP TABLE t_merge_filter;

-- Every integer width is affected, not only the low byte of the value.
DROP TABLE IF EXISTS t_merge_filter_wide;

CREATE TABLE t_merge_filter_wide (id Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_merge_filter_wide VALUES (1)(256)(65536)(4294967296);

SELECT count() FROM t_merge_filter_wide AS l LEFT JOIN t_merge_filter_wide AS r ON l.id = r.id
WHERE r.id AND l.id = r.id;

DROP TABLE t_merge_filter_wide;

-- A `NULL` condition is false, and the conversion keeps the value nullable.
DROP TABLE IF EXISTS t_merge_filter_null;

CREATE TABLE t_merge_filter_null (id UInt32, x Nullable(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_merge_filter_null SELECT number, if(number = 512, NULL, number) FROM numbers(600);

SELECT count() FROM t_merge_filter_null AS l LEFT JOIN t_merge_filter_null AS r ON l.id = r.id
WHERE r.x AND l.id = r.id;

SELECT l.id FROM t_merge_filter_null AS l LEFT JOIN t_merge_filter_null AS r ON l.id = r.id
WHERE r.x AND l.id = r.id AND l.id IN (0, 255, 256, 512)
ORDER BY l.id;

DROP TABLE t_merge_filter_null;

-- A WHERE condition that is not boolean must keep the semantics of `and` when the other conjuncts
-- are merged into the JOIN condition: every non-zero value is true, not only the ones that survive
-- a cast to UInt8.

SET query_plan_merge_filter_into_join_condition = 1;

DROP TABLE IF EXISTS t_merge_filter;

CREATE TABLE t_merge_filter (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_merge_filter SELECT number FROM numbers(600);

SELECT count() FROM t_merge_filter AS l LEFT JOIN t_merge_filter AS r ON l.id = r.id
WHERE r.id AND l.id = r.id;

SELECT count() FROM t_merge_filter AS l LEFT JOIN t_merge_filter AS r ON l.id = r.id
WHERE r.id AND l.id = r.id
SETTINGS query_plan_merge_filter_into_join_condition = 0;

-- The INNER JOIN shape from the linked issue reaches the merge pass by a different route;
-- the filter push down is disabled so that the filter stays above the JOIN for the merge pass.
SELECT count() FROM t_merge_filter AS l INNER JOIN t_merge_filter AS r ON l.id = r.id
WHERE r.id AND l.id = r.id
SETTINGS query_plan_filter_push_down = 0;

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

-- A plain `UInt8` conjunct is not a normalized boolean either: a value like 2 passes the filter,
-- but the value of the predicate in the result must still be 0 or 1.
DROP TABLE IF EXISTS t_merge_filter_u8;

CREATE TABLE t_merge_filter_u8 (id UInt32, u UInt8, n Nullable(UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_merge_filter_u8 SELECT number, if(number % 3 = 0, 0, 2), if(number % 3 = 0, NULL, 2) FROM numbers(10);

SELECT l.id, (r.u AND l.id = r.id) AS c
FROM t_merge_filter_u8 AS l LEFT JOIN t_merge_filter_u8 AS r ON l.id = r.id
WHERE r.u AND l.id = r.id
ORDER BY l.id;

SELECT l.id, (r.u AND l.id = r.id) AS c
FROM t_merge_filter_u8 AS l LEFT JOIN t_merge_filter_u8 AS r ON l.id = r.id
WHERE r.u AND l.id = r.id
ORDER BY l.id
SETTINGS query_plan_merge_filter_into_join_condition = 0;

-- The same for `Nullable(UInt8)`.
SELECT l.id, (r.n AND l.id = r.id) AS c
FROM t_merge_filter_u8 AS l LEFT JOIN t_merge_filter_u8 AS r ON l.id = r.id
WHERE r.n AND l.id = r.id
ORDER BY l.id;

DROP TABLE t_merge_filter_u8;

-- A `LowCardinality` conjunct: `and` returns a full column, so the conversion also changes
-- the type of the leftover conjunct.
DROP TABLE IF EXISTS t_merge_filter_lc;

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_merge_filter_lc (id UInt32, x LowCardinality(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_merge_filter_lc SELECT number, number FROM numbers(600);

SELECT count() FROM t_merge_filter_lc AS l LEFT JOIN t_merge_filter_lc AS r ON l.id = r.id
WHERE r.x AND l.id = r.id;

SELECT l.id, (r.x AND l.id = r.id) AS c FROM t_merge_filter_lc AS l LEFT JOIN t_merge_filter_lc AS r ON l.id = r.id
WHERE r.x AND l.id = r.id AND l.id IN (0, 255, 256, 512)
ORDER BY l.id;

DROP TABLE t_merge_filter_lc;

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

-- A constant `IN` set on the source side must be lifted onto the target side, where it prunes
-- the primary key just like a comparison does.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS lift_in_src;
DROP TABLE IF EXISTS lift_in_dst;

CREATE TABLE lift_in_src (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE lift_in_dst (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;

INSERT INTO lift_in_src SELECT number FROM numbers(1000);
INSERT INTO lift_in_dst SELECT number FROM numbers(1000);

SELECT count()
FROM (SELECT * FROM lift_in_src WHERE k IN (10, 500)) AS s
INNER JOIN lift_in_dst AS d ON s.k = d.k;

-- The lifted set prunes the target read: two granules instead of the whole table.
SELECT countIf(explain LIKE '%Granules: 2/125%') > 0
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT count()
    FROM (SELECT * FROM lift_in_src WHERE k IN (10, 500)) AS s
    INNER JOIN lift_in_dst AS d ON s.k = d.k
);

-- Without the lift the target is read in full.
SELECT countIf(explain LIKE '%Granules: 125/125%') > 0
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT count()
    FROM (SELECT * FROM lift_in_src WHERE k IN (10, 500)) AS s
    INNER JOIN lift_in_dst AS d ON s.k = d.k
    SETTINGS query_plan_lift_predicate_across_join = 0
);

DROP TABLE lift_in_src;
DROP TABLE lift_in_dst;

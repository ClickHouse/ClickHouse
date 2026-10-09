-- A constant `IN` set prunes the target primary key like a comparison does

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS prop_in_src;
DROP TABLE IF EXISTS prop_in_dst;

CREATE TABLE prop_in_src (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE prop_in_dst (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;

INSERT INTO prop_in_src SELECT number FROM numbers(1000);
INSERT INTO prop_in_dst SELECT number FROM numbers(1000);

SELECT count()
FROM (SELECT * FROM prop_in_src WHERE k IN (10, 500)) AS s
INNER JOIN prop_in_dst AS d ON s.k = d.k;

-- Both reads prune to two granules: the source by its own set, the target by the copy
SELECT countIf(explain LIKE '%Granules: 2/125%') = 2
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT count()
    FROM (SELECT * FROM prop_in_src WHERE k IN (10, 500)) AS s
    INNER JOIN prop_in_dst AS d ON s.k = d.k
);

-- Without the pass the target is read in full
SELECT countIf(explain LIKE '%Granules: 125/125%') > 0
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT count()
    FROM (SELECT * FROM prop_in_src WHERE k IN (10, 500)) AS s
    INNER JOIN prop_in_dst AS d ON s.k = d.k
    SETTINGS query_plan_propagate_predicate_across_join = 0
);

DROP TABLE prop_in_src;
DROP TABLE prop_in_dst;

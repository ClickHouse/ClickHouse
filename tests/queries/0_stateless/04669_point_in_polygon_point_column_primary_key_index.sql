-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- https://github.com/ClickHouse/ClickHouse/issues/54805

-- EXPLAIN output may differ between old and new format
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS points;
CREATE TABLE points (coord Point) ENGINE = MergeTree ORDER BY coord SETTINGS index_granularity = 1000;

INSERT INTO points SELECT (number, number) FROM numbers(100000);

SELECT count()
FROM points
WHERE pointInPolygon(coord, [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]);

-- The primary key index must be used
SELECT count()
FROM points
WHERE pointInPolygon(coord, [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count()
    FROM points
    WHERE pointInPolygon(coord, [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- A polygon that does not intersect the data at all
SELECT count()
FROM points
WHERE pointInPolygon(coord, [(200000, 200000), (200000, 300000), (300000, 300000), (300000, 200000)]);

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count()
    FROM points
    WHERE pointInPolygon(coord, [(200000, 200000), (200000, 300000), (300000, 300000), (300000, 200000)])
) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%';

DROP TABLE points;

-- The second coordinate is used for pruning when the first coordinate is fixed
DROP TABLE IF EXISTS points_fixed_x;
CREATE TABLE points_fixed_x (coord Point) ENGINE = MergeTree ORDER BY coord SETTINGS index_granularity = 1000;

INSERT INTO points_fixed_x SELECT (5, number) FROM numbers(100000);

SELECT count()
FROM points_fixed_x
WHERE pointInPolygon(coord, [(0, 30000), (10, 30000), (10, 70000), (0, 70000)]);

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count()
    FROM points_fixed_x
    WHERE pointInPolygon(coord, [(0, 30000), (10, 30000), (10, 70000), (0, 70000)])
) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%';

DROP TABLE points_fixed_x;

-- The key column of type Tuple (not Point) works as well, and a composite key too
DROP TABLE IF EXISTS points_tuple;
CREATE TABLE points_tuple (id UInt64, coord Tuple(Float64, Float64)) ENGINE = MergeTree ORDER BY (coord, id) SETTINGS index_granularity = 1000;

INSERT INTO points_tuple SELECT number, (number, number) FROM numbers(100000);

SELECT count()
FROM points_tuple
WHERE pointInPolygon(coord, [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]);

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count()
    FROM points_tuple
    WHERE pointInPolygon(coord, [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%';

DROP TABLE points_tuple;

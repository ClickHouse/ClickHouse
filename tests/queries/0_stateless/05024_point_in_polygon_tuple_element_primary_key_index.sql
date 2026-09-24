-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings

-- EXPLAIN output may differ between old and new format
SET explain_query_plan_default = 'legacy';

-- Part-level statistics can prune with the same condition and add their own block to
-- EXPLAIN indexes; do not materialize statistics, so the output does not depend on the
-- randomized statistics settings
SET materialize_statistics_on_insert = 0;

-- The point as the two elements of a tuple-typed (Point) key column
DROP TABLE IF EXISTS points_tuple;
CREATE TABLE points_tuple (coord Point) ENGINE = MergeTree ORDER BY coord SETTINGS index_granularity = 1000;

INSERT INTO points_tuple SELECT (number, number) FROM numbers(100000);

-- pointInPolygon over the two elements of the tuple-typed key column resolves to the
-- single-key-column form: the point's bounding box is derived from the range of the tuple key
SELECT count() FROM points_tuple WHERE pointInPolygon((coord.1, coord.2), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]);
SELECT count() FROM points_tuple WHERE pointInPolygon((coord.1, coord.2), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM points_tuple WHERE pointInPolygon((coord.1, coord.2), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- Swapped elements do not form the point: no index analysis, correct result
SELECT count() FROM points_tuple WHERE pointInPolygon((coord.2, coord.1), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]);

-- The element index constants may be of a signed type
SELECT count() FROM points_tuple WHERE pointInPolygon((tupleElement(coord, toInt64(1)), tupleElement(coord, toInt64(2))), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]) SETTINGS force_primary_key = 1;

DROP TABLE points_tuple;

-- The point as the two named elements of a tuple-typed key column
DROP TABLE IF EXISTS named_points;
CREATE TABLE named_points (p Tuple(x Float64, y Float64)) ENGINE = MergeTree ORDER BY p SETTINGS index_granularity = 1000;

INSERT INTO named_points SELECT (number, number) FROM numbers(100000);

SELECT count() FROM named_points WHERE pointInPolygon((p.x, p.y), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]);
SELECT count() FROM named_points WHERE pointInPolygon((p.x, p.y), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM named_points WHERE pointInPolygon((p.x, p.y), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- The explicit tupleElement form with element names
SELECT count() FROM named_points WHERE pointInPolygon((tupleElement(p, 'x'), tupleElement(p, 'y')), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]) SETTINGS force_primary_key = 1;

DROP TABLE named_points;

-- A tuple key column of three elements is not a point: no index analysis, correct result
DROP TABLE IF EXISTS points3;
CREATE TABLE points3 (t Tuple(Float64, Float64, Float64)) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1000;

INSERT INTO points3 SELECT (number, number, number) FROM numbers(10000);

SELECT count() FROM points3 WHERE pointInPolygon((t.1, t.2), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]);

DROP TABLE points3;

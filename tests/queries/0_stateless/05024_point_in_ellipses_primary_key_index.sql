-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings

-- EXPLAIN output may differ between old and new format
SET explain_query_plan_default = 'legacy';

-- Part-level statistics can prune with the same condition and add their own block to
-- EXPLAIN indexes (and the PrimaryKey block then reports 0/0 parts); do not materialize
-- statistics, so the output does not depend on the randomized statistics settings
SET materialize_statistics_on_insert = 0;

DROP TABLE IF EXISTS points_xy;
CREATE TABLE points_xy (x Float64, y Float64) ENGINE = MergeTree ORDER BY (x, y) SETTINGS index_granularity = 1000;

INSERT INTO points_xy SELECT number, number FROM numbers(100000);

-- The union bounding box of the ellipses is intersected with the granule range.
-- Diagonal points (n, n) are inside the ellipse for 2 * n^2 <= 5000^2, i.e. n <= 3535
SELECT count() FROM points_xy WHERE pointInEllipses(x, y, 0., 0., 5000., 5000.);
SELECT count() FROM points_xy WHERE pointInEllipses(x, y, 0., 0., 5000., 5000.) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM points_xy WHERE pointInEllipses(x, y, 0., 0., 5000., 5000.)
) WHERE explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- A point inside the union bounding box but outside the ellipse itself: the index only
-- over-approximates with the box, the function's quadratic check rejects the point
-- (2 * 4000^2 > 5000^2)
SELECT pointInEllipses(4000., 4000., 0., 0., 5000., 5000.);

-- A union of two ellipses
SELECT count() FROM points_xy WHERE pointInEllipses(x, y, 0., 0., 5000., 5000., 50000., 50000., 1000., 2000.);

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM points_xy WHERE pointInEllipses(x, y, 0., 0., 5000., 5000., 50000., 50000., 1000., 2000.)
) WHERE explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- An ellipse that does not intersect the data at all
SELECT count() FROM points_xy WHERE pointInEllipses(x, y, 300000., 300000., 100., 100.);

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM points_xy WHERE pointInEllipses(x, y, 300000., 300000., 100., 100.)
) WHERE explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- Empty ellipses match no point at all (the function's own bounding-box check never passes):
-- a negative semi-axis, a zero semi-axis, a NaN parameter.
-- The condition folds to always-false
SELECT count() FROM points_xy WHERE pointInEllipses(x, y, 5., 5., -10., 10.);
SELECT count() FROM points_xy WHERE pointInEllipses(x, y, 5., 5., 0., 10.);
SELECT count() FROM points_xy WHERE pointInEllipses(x, y, nan, 5., 10., 10.);

-- A mix of an empty and a normal ellipse: only the normal one constrains the box
SELECT count() FROM points_xy WHERE pointInEllipses(x, y, 5., 5., -10., 10., 0., 0., 5000., 5000.);

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM points_xy WHERE pointInEllipses(x, y, 5., 5., -10., 10., 0., 0., 5000., 5000.)
) WHERE explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- An infinite parameter disables pruning, the result stays correct
SELECT count() FROM points_xy WHERE pointInEllipses(x, y, 0., 0., inf, 10.);

-- NOT: no pruning is claimed, the result stays correct
SELECT count() FROM points_xy WHERE NOT pointInEllipses(x, y, 0., 0., 5000., 5000.);

DROP TABLE points_xy;

-- The point as the two elements of a tuple-typed (Point) key column
DROP TABLE IF EXISTS points_tuple;
CREATE TABLE points_tuple (coord Point) ENGINE = MergeTree ORDER BY coord SETTINGS index_granularity = 1000;

INSERT INTO points_tuple SELECT (number, number) FROM numbers(100000);

SELECT count() FROM points_tuple WHERE pointInEllipses(coord.1, coord.2, 0., 0., 5000., 5000.);
SELECT count() FROM points_tuple WHERE pointInEllipses(coord.1, coord.2, 0., 0., 5000., 5000.) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM points_tuple WHERE pointInEllipses(coord.1, coord.2, 0., 0., 5000., 5000.)
) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- pointInPolygon over the two elements of the tuple-typed key column
SELECT count() FROM points_tuple WHERE pointInPolygon((coord.1, coord.2), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]);
SELECT count() FROM points_tuple WHERE pointInPolygon((coord.1, coord.2), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM points_tuple WHERE pointInPolygon((coord.1, coord.2), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%';

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

DROP TABLE named_points;

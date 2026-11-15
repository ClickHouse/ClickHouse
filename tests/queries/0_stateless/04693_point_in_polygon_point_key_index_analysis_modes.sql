-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- Follow-up coverage for https://github.com/ClickHouse/ClickHouse/pull/112956

-- `pointInPolygon` over a whole key column (or key expression) of type `Point` / `Tuple` is handled by
-- two separate `KeyCondition::checkInHyperrectangle` overloads, selected by
-- `use_lightweight_primary_key_index_analysis`. `clickhouse-test` randomizes that setting, so every query
-- below pins it explicitly and each case is checked in both modes.

-- EXPLAIN output may differ between old and new format
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS points;
CREATE TABLE points (coord Point) ENGINE = MergeTree ORDER BY coord SETTINGS index_granularity = 1000;

INSERT INTO points SELECT (number, number) FROM numbers(100000);

SELECT 'Point key column, dense index analysis';

SELECT count()
FROM points
WHERE pointInPolygon(coord, [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
SETTINGS use_lightweight_primary_key_index_analysis = 0, force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count()
    FROM points
    WHERE pointInPolygon(coord, [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
    SETTINGS use_lightweight_primary_key_index_analysis = 0
) WHERE explain LIKE '%Granules%';

SELECT 'Point key column, lightweight index analysis';

SELECT count()
FROM points
WHERE pointInPolygon(coord, [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
SETTINGS use_lightweight_primary_key_index_analysis = 1, force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count()
    FROM points
    WHERE pointInPolygon(coord, [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
    SETTINGS use_lightweight_primary_key_index_analysis = 1
) WHERE explain LIKE '%Granules%';

-- A polygon that does not intersect the data at all - everything must be pruned in both modes.

SELECT 'Disjoint polygon, dense index analysis';

SELECT count()
FROM points
WHERE pointInPolygon(coord, [(200000, 200000), (200000, 300000), (300000, 300000), (300000, 200000)])
SETTINGS use_lightweight_primary_key_index_analysis = 0;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count()
    FROM points
    WHERE pointInPolygon(coord, [(200000, 200000), (200000, 300000), (300000, 300000), (300000, 200000)])
    SETTINGS use_lightweight_primary_key_index_analysis = 0
) WHERE explain LIKE '%Granules%';

SELECT 'Disjoint polygon, lightweight index analysis';

SELECT count()
FROM points
WHERE pointInPolygon(coord, [(200000, 200000), (200000, 300000), (300000, 300000), (300000, 200000)])
SETTINGS use_lightweight_primary_key_index_analysis = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count()
    FROM points
    WHERE pointInPolygon(coord, [(200000, 200000), (200000, 300000), (300000, 300000), (300000, 200000)])
    SETTINGS use_lightweight_primary_key_index_analysis = 1
) WHERE explain LIKE '%Granules%';

DROP TABLE points;

-- The second coordinate is only usable for pruning when the first coordinate is fixed,
-- because tuples are ordered lexicographically. Check both modes.

DROP TABLE IF EXISTS points_fixed_x;
CREATE TABLE points_fixed_x (coord Point) ENGINE = MergeTree ORDER BY coord SETTINGS index_granularity = 1000;

INSERT INTO points_fixed_x SELECT (5, number) FROM numbers(100000);

SELECT 'Fixed first coordinate, dense index analysis';

SELECT count()
FROM points_fixed_x
WHERE pointInPolygon(coord, [(0, 30000), (10, 30000), (10, 70000), (0, 70000)])
SETTINGS use_lightweight_primary_key_index_analysis = 0;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count()
    FROM points_fixed_x
    WHERE pointInPolygon(coord, [(0, 30000), (10, 30000), (10, 70000), (0, 70000)])
    SETTINGS use_lightweight_primary_key_index_analysis = 0
) WHERE explain LIKE '%Granules%';

SELECT 'Fixed first coordinate, lightweight index analysis';

SELECT count()
FROM points_fixed_x
WHERE pointInPolygon(coord, [(0, 30000), (10, 30000), (10, 70000), (0, 70000)])
SETTINGS use_lightweight_primary_key_index_analysis = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count()
    FROM points_fixed_x
    WHERE pointInPolygon(coord, [(0, 30000), (10, 30000), (10, 70000), (0, 70000)])
    SETTINGS use_lightweight_primary_key_index_analysis = 1
) WHERE explain LIKE '%Granules%';

DROP TABLE points_fixed_x;

-- A key *expression* producing a tuple of two coordinates, rather than a physical column of type `Point`.
-- Note that `ORDER BY tuple(x, y)` alone is expanded by `extractKeyExpressionList` into the two key
-- columns `x` and `y`, which takes the other code path; to get a single key column holding the tuple,
-- the tuple has to be one element of a composite key.

DROP TABLE IF EXISTS points_key_expression;
CREATE TABLE points_key_expression (x Float64, y Float64, id UInt64)
ENGINE = MergeTree ORDER BY (tuple(x, y), id) SETTINGS index_granularity = 1000;

INSERT INTO points_key_expression SELECT number, number, number FROM numbers(100000);

SELECT 'Key expression, dense index analysis';

SELECT count()
FROM points_key_expression
WHERE pointInPolygon((x, y), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
SETTINGS use_lightweight_primary_key_index_analysis = 0, force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count()
    FROM points_key_expression
    WHERE pointInPolygon((x, y), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
    SETTINGS use_lightweight_primary_key_index_analysis = 0
) WHERE explain LIKE '%Granules%';

SELECT 'Key expression, lightweight index analysis';

SELECT count()
FROM points_key_expression
WHERE pointInPolygon((x, y), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
SETTINGS use_lightweight_primary_key_index_analysis = 1, force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count()
    FROM points_key_expression
    WHERE pointInPolygon((x, y), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
    SETTINGS use_lightweight_primary_key_index_analysis = 1
) WHERE explain LIKE '%Granules%';

DROP TABLE points_key_expression;

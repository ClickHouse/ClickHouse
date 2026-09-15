-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings

-- EXPLAIN output may differ between old and new format
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS points;
CREATE TABLE points (coord Point) ENGINE = MergeTree ORDER BY coord SETTINGS index_granularity = 1000;

INSERT INTO points SELECT (number, number) FROM numbers(100000);

-- The first element of a tuple-typed key column is a non-strictly monotonic function
-- of the tuple (tuples are ordered lexicographically), so the primary key index is used
SELECT count() FROM points WHERE coord.1 <= 25000;
SELECT count() FROM points WHERE coord.1 <= 25000 SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM points WHERE coord.1 <= 25000
) WHERE explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- Equality
SELECT count() FROM points WHERE coord.1 = 5000 SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM points WHERE coord.1 = 5000
) WHERE explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- BETWEEN
SELECT count() FROM points WHERE coord.1 BETWEEN 10000 AND 20000 SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM points WHERE coord.1 BETWEEN 10000 AND 20000
) WHERE explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- NOT
SELECT count() FROM points WHERE NOT (coord.1 < 5000) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM points WHERE NOT (coord.1 < 5000)
) WHERE explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- A monotonic function chain on top of the tuple element
SELECT count() FROM points WHERE coord.1 + 1 <= 25001 SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM points WHERE coord.1 + 1 <= 25001
) WHERE explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- The explicit tupleElement form works the same as the dot form
SELECT count() FROM points WHERE tupleElement(coord, 1) <= 25000 SETTINGS force_primary_key = 1;

-- The element index constant may be of a signed type
SELECT count() FROM points WHERE tupleElement(coord, toInt64(1)) <= 25000 SETTINGS force_primary_key = 1;

-- The second element is not monotonic w.r.t. the tuple order:
-- the result is correct, and the index cannot be used
SELECT count() FROM points WHERE coord.2 <= 25000;
SELECT count() FROM points WHERE coord.2 <= 25000 SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

DROP TABLE points;

-- A named tuple: access to the first element by name uses the index, to the second does not
DROP TABLE IF EXISTS named_points;
CREATE TABLE named_points (p Tuple(x Float64, y Float64)) ENGINE = MergeTree ORDER BY p SETTINGS index_granularity = 1000;

INSERT INTO named_points SELECT (number, number) FROM numbers(100000);

SELECT count() FROM named_points WHERE p.x <= 25000 SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM named_points WHERE p.x <= 25000
) WHERE explain LIKE '%Parts%' OR explain LIKE '%Granules%';

-- The explicit tupleElement form with the element name goes through a separate branch
-- of the analysis (a String constant instead of a subcolumn read), pin it as well
SELECT count() FROM named_points WHERE tupleElement(p, 'x') <= 25000 SETTINGS force_primary_key = 1;

SELECT count() FROM named_points WHERE p.y <= 25000;
SELECT count() FROM named_points WHERE p.y <= 25000 SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

DROP TABLE named_points;

-- The key defined as the tuple element itself: the exact-match path must keep working
DROP TABLE IF EXISTS element_key;
CREATE TABLE element_key (coord Point) ENGINE = MergeTree ORDER BY coord.1 SETTINGS index_granularity = 1000;

INSERT INTO element_key SELECT (number, number) FROM numbers(100000);

SELECT count() FROM element_key WHERE coord.1 <= 25000 SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM element_key WHERE coord.1 <= 25000
) WHERE explain LIKE '%Parts%' OR explain LIKE '%Granules%';

DROP TABLE element_key;

-- tupleElement over a non-tuple (Array(Tuple)) key column: no index analysis, correct result
DROP TABLE IF EXISTS array_key;
CREATE TABLE array_key (arr Array(Tuple(UInt64, UInt64))) ENGINE = MergeTree ORDER BY arr;

INSERT INTO array_key SELECT [(number, number)] FROM numbers(1000);

SELECT count() FROM array_key WHERE tupleElement(arr, 1) = [5];

DROP TABLE array_key;

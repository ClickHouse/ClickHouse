-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

-- { echo }

DROP TABLE IF EXISTS test_morton;
DROP TABLE IF EXISTS test_hilbert;
DROP TABLE IF EXISTS test_polygon;

CREATE TABLE test_morton (x UInt32, y UInt32, z UInt8) ENGINE = MergeTree
ORDER BY (mortonEncode(x, y), z)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_morton SELECT number % 8, intDiv(number, 8), number % 4 FROM numbers(64);

-- Ranges over space-filling curve arguments.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_morton WHERE x BETWEEN 2 AND 3) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_morton WHERE x BETWEEN 2 AND 3 SETTINGS force_primary_key = 1;
SELECT count() FROM test_morton WHERE x BETWEEN 2 AND 3 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_morton WHERE y BETWEEN 2 AND 3) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_morton WHERE y BETWEEN 2 AND 3 SETTINGS force_primary_key = 1;
SELECT count() FROM test_morton WHERE y BETWEEN 2 AND 3 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Conjunction over both curve arguments folds into one hyperrectangle.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_morton WHERE x BETWEEN 2 AND 3 AND y BETWEEN 2 AND 3) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_morton WHERE x BETWEEN 2 AND 3 AND y BETWEEN 2 AND 3 SETTINGS force_primary_key = 1;
SELECT count() FROM test_morton WHERE x BETWEEN 2 AND 3 AND y BETWEEN 2 AND 3 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Curve argument predicate combined with a plain key column predicate.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_morton WHERE x = 2 AND z = 2) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_morton WHERE x = 2 AND z = 2 SETTINGS force_primary_key = 1;
SELECT count() FROM test_morton WHERE x = 2 AND z = 2 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Disjunction over curve arguments.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_morton WHERE x < 2 OR y < 2) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_morton WHERE x < 2 OR y < 2;
SELECT count() FROM test_morton WHERE x < 2 OR y < 2 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

CREATE TABLE test_hilbert (x UInt32, y UInt32) ENGINE = MergeTree
ORDER BY hilbertEncode(x, y)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_hilbert SELECT number % 8, intDiv(number, 8) FROM numbers(64);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_hilbert WHERE x BETWEEN 2 AND 3 AND y BETWEEN 2 AND 3) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_hilbert WHERE x BETWEEN 2 AND 3 AND y BETWEEN 2 AND 3 SETTINGS force_primary_key = 1;
SELECT count() FROM test_hilbert WHERE x BETWEEN 2 AND 3 AND y BETWEEN 2 AND 3 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- pointInPolygon over two key columns.
CREATE TABLE test_polygon (x UInt32, y UInt32, z UInt8) ENGINE = MergeTree
ORDER BY (x, y)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_polygon SELECT number % 10, intDiv(number, 10), number % 2 FROM numbers(100);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_polygon WHERE pointInPolygon((x, y), [(0, 0), (8, 4), (5, 8), (0, 2)])) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_polygon WHERE pointInPolygon((x, y), [(0, 0), (8, 4), (5, 8), (0, 2)]);
SELECT count() FROM test_polygon WHERE pointInPolygon((x, y), [(0, 0), (8, 4), (5, 8), (0, 2)]) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Polygon with holes: holes are ignored for index analysis but applied by the filter.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_polygon WHERE pointInPolygon((x, y), [(0, 0), (8, 4), (5, 8), (0, 2)], [(2, 2), (3, 2), (3, 3), (2, 3)])) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_polygon WHERE pointInPolygon((x, y), [(0, 0), (8, 4), (5, 8), (0, 2)], [(2, 2), (3, 2), (3, 3), (2, 3)]);
SELECT count() FROM test_polygon WHERE pointInPolygon((x, y), [(0, 0), (8, 4), (5, 8), (0, 2)], [(2, 2), (3, 2), (3, 3), (2, 3)]) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Polygon predicate combined with a prunable key range.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_polygon WHERE pointInPolygon((x, y), [(0, 0), (8, 4), (5, 8), (0, 2)]) AND x < 3) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM test_polygon WHERE pointInPolygon((x, y), [(0, 0), (8, 4), (5, 8), (0, 2)]) AND x < 3;
SELECT count() FROM test_polygon WHERE pointInPolygon((x, y), [(0, 0), (8, 4), (5, 8), (0, 2)]) AND x < 3 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_morton;
DROP TABLE test_hilbert;
DROP TABLE test_polygon;

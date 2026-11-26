-- Tags: no-random-merge-tree-settings
-- Test pointInPolygon over a single Point key column on the regular (non-lightweight) primary key analysis path.

DROP TABLE IF EXISTS points_non_lightweight;
CREATE TABLE points_non_lightweight (coord Point) ENGINE = MergeTree ORDER BY coord SETTINGS index_granularity = 1000;

INSERT INTO points_non_lightweight SELECT (number, number) FROM numbers(100000);

-- Intersecting polygon: the primary key index must prune granules, so the read stays well under the full 100000 rows.
SELECT count()
FROM points_non_lightweight
WHERE pointInPolygon(coord, [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
SETTINGS use_lightweight_primary_key_index_analysis = 0, force_primary_key = 1, max_rows_to_read = 40000;

-- Disjoint polygon: every granule is pruned, so no rows are read at all.
SELECT count()
FROM points_non_lightweight
WHERE pointInPolygon(coord, [(200000, 200000), (200000, 300000), (300000, 300000), (300000, 200000)])
SETTINGS use_lightweight_primary_key_index_analysis = 0, force_primary_key = 1, max_rows_to_read = 1;

DROP TABLE points_non_lightweight;

-- Asymmetric case: the first coordinate is fixed (x = 100), only the second varies, and the
-- polygon has DIFFERENT x/y extents (x in [0, 200], y in [0, 25000]). The diagonal case above
-- cannot distinguish first- from second-coordinate pruning, and a symmetric square cannot catch a
-- coordinate swap. Correct pruning uses the second coordinate and reads 26 granules (26000 rows),
-- returning 25001 under the cap. A regression that DROPS the y bound in the regular overload's
-- tupleRangeToBoundingBox prunes nothing and reads all 100000 rows, exceeding max_rows_to_read. A
-- regression that SWAPS x and y makes the granule bbox x = [y_lo, y_hi], y = [100, 100]; only
-- granule 0 intersects the narrow x = [0, 200] extent, so the query reads 1000 rows and returns
-- 1000 instead of 25001, failing the reference.
DROP TABLE IF EXISTS points_non_lightweight_fixed_x;
CREATE TABLE points_non_lightweight_fixed_x (coord Point) ENGINE = MergeTree ORDER BY coord SETTINGS index_granularity = 1000;

INSERT INTO points_non_lightweight_fixed_x SELECT (100, number) FROM numbers(100000);

SELECT count()
FROM points_non_lightweight_fixed_x
WHERE pointInPolygon(coord, [(0, 0), (0, 25000), (200, 25000), (200, 0)])
SETTINGS use_lightweight_primary_key_index_analysis = 0, force_primary_key = 1, max_rows_to_read = 40000;

DROP TABLE points_non_lightweight_fixed_x;

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

-- Asymmetric case: the first coordinate is fixed (x = 100) and only the second varies, so
-- pruning MUST use the second coordinate of the bounding box. The diagonal case above cannot
-- distinguish first- from second-coordinate pruning; here every row shares x = 100 which lies
-- inside the polygon's x range, so a regression that drops or swaps the y bound in the regular
-- overload's tupleRangeToBoundingBox would prune nothing and read all 100000 rows, exceeding
-- max_rows_to_read. Correct behaviour prunes on y and stays well under the cap.
DROP TABLE IF EXISTS points_non_lightweight_fixed_x;
CREATE TABLE points_non_lightweight_fixed_x (coord Point) ENGINE = MergeTree ORDER BY coord SETTINGS index_granularity = 1000;

INSERT INTO points_non_lightweight_fixed_x SELECT (100, number) FROM numbers(100000);

SELECT count()
FROM points_non_lightweight_fixed_x
WHERE pointInPolygon(coord, [(0, 0), (0, 25000), (25000, 25000), (25000, 0)])
SETTINGS use_lightweight_primary_key_index_analysis = 0, force_primary_key = 1, max_rows_to_read = 40000;

DROP TABLE points_non_lightweight_fixed_x;

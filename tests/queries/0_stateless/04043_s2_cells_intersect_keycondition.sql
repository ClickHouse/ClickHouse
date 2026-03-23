-- Tags: no-fasttest
-- Test that s2CellsIntersect uses the S2 key condition, and that the
-- existing s2RectContains/s2CapContains still work after unifying them
-- into FUNCTION_S2_COVERING.

SET allow_experimental_s2_keycondition = 1;

DROP TABLE IF EXISTS t_s2_cells;
CREATE TABLE t_s2_cells (id UInt32, cell_id UInt64)
ENGINE = MergeTree ORDER BY cell_id
SETTINGS index_granularity = 128;

INSERT INTO t_s2_cells SELECT number, geoToS2(-74.0 + (number % 100) * 0.005, 40.7 + (number / 100) * 0.005) FROM numbers(1000);
OPTIMIZE TABLE t_s2_cells FINAL;

-- s2CellsIntersect(a, b) is true when cells share area (parent/child or same cell).
-- Use a known intersecting pair from the S2 docs:
-- 9926595209846587392 and 9926594385212866560 intersect.
-- Insert the first one explicitly so we can query for it.
INSERT INTO t_s2_cells VALUES (10001, 9926595209846587392);
OPTIMIZE TABLE t_s2_cells FINAL;

-- 1. s2CellsIntersect: key column first
SELECT count() FROM t_s2_cells
WHERE s2CellsIntersect(cell_id, 9926594385212866560);

-- 2. s2CellsIntersect: key column second (commutative)
SELECT count() FROM t_s2_cells
WHERE s2CellsIntersect(9926594385212866560, cell_id);

-- 3. Both argument orders give same result
SELECT
    (SELECT count() FROM t_s2_cells WHERE s2CellsIntersect(cell_id, 9926594385212866560))
    =
    (SELECT count() FROM t_s2_cells WHERE s2CellsIntersect(9926594385212866560, cell_id));

-- 4. No false negatives: index result matches full scan
SELECT
    (SELECT count() FROM t_s2_cells WHERE s2CellsIntersect(cell_id, 9926594385212866560))
    =
    (SELECT count() FROM t_s2_cells WHERE s2CellsIntersect(cell_id, 9926594385212866560) SETTINGS use_primary_key = 0);

-- 5. EXPLAIN: s2CellsIntersect shows in key condition and prunes granules
EXPLAIN indexes = 1 SELECT count() FROM t_s2_cells
WHERE s2CellsIntersect(cell_id, 9926594385212866560);

-- 6. EXPLAIN: s2RectContains still works after refactoring
EXPLAIN indexes = 1 SELECT count() FROM t_s2_cells
WHERE s2RectContains(geoToS2(-74.0, 40.7), geoToS2(-73.9, 40.8), cell_id);

-- 7. EXPLAIN: s2CapContains still works after refactoring
EXPLAIN indexes = 1 SELECT count() FROM t_s2_cells
WHERE s2CapContains(geoToS2(-73.98, 40.75), 0.01, cell_id);

DROP TABLE t_s2_cells;

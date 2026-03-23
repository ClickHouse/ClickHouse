-- Tags: no-fasttest
-- Test S2 key condition: s2RectContains, s2CapContains, s2CellsIntersect
-- for primary-key index pruning, plus the `s2_max_covering_cells` setting.

SET allow_experimental_s2_keycondition = 1;

-- ── Table 1: large grid for no-false-negatives checks ────────────────────────

DROP TABLE IF EXISTS t_s2_idx;
CREATE TABLE t_s2_idx
(
    id UInt64,
    s2_loc UInt64
)
ENGINE = MergeTree
ORDER BY s2_loc
SETTINGS index_granularity = 100;

INSERT INTO t_s2_idx
SELECT
    number,
    geoToS2(
        toFloat64(number % 360) - 180.0,
        toFloat64(intDiv(number, 360) % 180) - 90.0
    )
FROM numbers(64800); -- 360 * 180 = full grid

-- 1. s2RectContains: pruning must not lose any rows
SELECT
    (SELECT count() FROM t_s2_idx
     WHERE s2RectContains(geoToS2(-5.0, -5.0), geoToS2(5.0, 5.0), s2_loc))
    =
    (SELECT count() FROM t_s2_idx
     WHERE s2RectContains(geoToS2(-5.0, -5.0), geoToS2(5.0, 5.0), s2_loc)
     SETTINGS allow_experimental_s2_keycondition = 0);

-- 2. s2CapContains: pruning must not lose any rows
SELECT
    (SELECT count() FROM t_s2_idx
     WHERE s2CapContains(geoToS2(0.0, 0.0), 5.0, s2_loc))
    =
    (SELECT count() FROM t_s2_idx
     WHERE s2CapContains(geoToS2(0.0, 0.0), 5.0, s2_loc)
     SETTINGS allow_experimental_s2_keycondition = 0);

DROP TABLE t_s2_idx;

-- ── Table 2: smaller grid for s2CellsIntersect + EXPLAIN checks ──────────────

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

-- 3. s2CellsIntersect: key column first
SELECT count() FROM t_s2_cells
WHERE s2CellsIntersect(cell_id, 9926594385212866560);

-- 4. s2CellsIntersect: key column second (commutative)
SELECT count() FROM t_s2_cells
WHERE s2CellsIntersect(9926594385212866560, cell_id);

-- 5. Both argument orders give same result
SELECT
    (SELECT count() FROM t_s2_cells WHERE s2CellsIntersect(cell_id, 9926594385212866560))
    =
    (SELECT count() FROM t_s2_cells WHERE s2CellsIntersect(9926594385212866560, cell_id));

-- 6. No false negatives: index result matches full scan
SELECT
    (SELECT count() FROM t_s2_cells WHERE s2CellsIntersect(cell_id, 9926594385212866560))
    =
    (SELECT count() FROM t_s2_cells WHERE s2CellsIntersect(cell_id, 9926594385212866560) SETTINGS use_primary_key = 0);

-- 7. EXPLAIN: s2CellsIntersect shows in key condition and prunes granules
EXPLAIN indexes = 1 SELECT count() FROM t_s2_cells
WHERE s2CellsIntersect(cell_id, 9926594385212866560);

-- 8. EXPLAIN: s2RectContains still works after refactoring
EXPLAIN indexes = 1 SELECT count() FROM t_s2_cells
WHERE s2RectContains(geoToS2(-74.0, 40.7), geoToS2(-73.9, 40.8), cell_id);

-- 9. EXPLAIN: s2CapContains still works after refactoring
EXPLAIN indexes = 1 SELECT count() FROM t_s2_cells
WHERE s2CapContains(geoToS2(-73.98, 40.75), 0.01, cell_id);

-- 10. s2_max_covering_cells setting is accepted (lower budget, same correctness)
SELECT count() FROM t_s2_cells
WHERE s2CellsIntersect(cell_id, 9926594385212866560)
SETTINGS s2_max_covering_cells = 8;

DROP TABLE t_s2_cells;

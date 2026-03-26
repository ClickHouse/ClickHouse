-- Tags: no-fasttest, no-parallel-replicas
-- Test ProjectionIndexS2: polygonsIntersectSpherical / polygonsWithinSpherical
-- rewrite to s2CellsIntersect for projection-based index filtering.
-- no-parallel-replicas: EXPLAIN output for granule counts differs under parallel replicas.

SET enable_s2_index_pruning = 1;
SET optimize_use_projection_filtering = 1;

-- ── Table 1: Polygon — polygonsIntersectSpherical ───────────────────────────────

DROP TABLE IF EXISTS t_s2_poly;
CREATE TABLE t_s2_poly
(
    id UInt64,
    polygon Polygon,
    PROJECTION s2_proj INDEX polygon TYPE s2(max_cells = 8, min_level = 15, max_level = 15)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_s2_poly VALUES
(1, CAST([[(0.0000, 0.0000), (0.0100, 0.0000), (0.0100, 0.0100), (0.0000, 0.0100), (0.0000, 0.0000)]], 'Polygon'));
INSERT INTO t_s2_poly VALUES
(2, CAST([[(0.0200, 0.0200), (0.0300, 0.0200), (0.0300, 0.0300), (0.0200, 0.0300), (0.0200, 0.0200)]], 'Polygon'));
INSERT INTO t_s2_poly VALUES
(3, CAST([[(30.0000, 30.0000), (30.0100, 30.0000), (30.0100, 30.0100), (30.0000, 30.0100), (30.0000, 30.0000)]], 'Polygon'));
INSERT INTO t_s2_poly VALUES
(4, CAST([[(-30.0000, -30.0000), (-29.9900, -30.0000), (-29.9900, -29.9900), (-30.0000, -29.9900), (-30.0000, -30.0000)]], 'Polygon'));
INSERT INTO t_s2_poly VALUES
(5, CAST([[(0.0050, 0.0050), (0.0150, 0.0050), (0.0150, 0.0150), (0.0050, 0.0150), (0.0050, 0.0050)]], 'Polygon'));

-- 1. No false negatives: polygonsIntersectSpherical
SELECT
    (SELECT count() FROM t_s2_poly
     WHERE polygonsIntersectSpherical(polygon,
        CAST([[(0.0060, 0.0060), (0.0120, 0.0060), (0.0120, 0.0120), (0.0060, 0.0120), (0.0060, 0.0060)]], 'Polygon')))
    =
    (SELECT count() FROM t_s2_poly
     WHERE polygonsIntersectSpherical(polygon,
        CAST([[(0.0060, 0.0060), (0.0120, 0.0060), (0.0120, 0.0120), (0.0060, 0.0120), (0.0060, 0.0060)]], 'Polygon'))
     SETTINGS enable_s2_index_pruning = 0);

-- 2. Correct row IDs returned
SELECT groupArray(id) FROM
(
    SELECT id FROM t_s2_poly
    WHERE polygonsIntersectSpherical(polygon,
        CAST([[(0.0060, 0.0060), (0.0120, 0.0060), (0.0120, 0.0120), (0.0060, 0.0120), (0.0060, 0.0060)]], 'Polygon'))
    ORDER BY id
);

-- 3. EXPLAIN: projection index in use for polygonsIntersectSpherical
EXPLAIN indexes = 1, projections = 1
SELECT id FROM t_s2_poly
WHERE polygonsIntersectSpherical(polygon,
    CAST([[(0.0060, 0.0060), (0.0120, 0.0060), (0.0120, 0.0120), (0.0060, 0.0120), (0.0060, 0.0060)]], 'Polygon'))
SETTINGS min_table_rows_to_use_projection_index = 0, max_projection_rows_to_use_projection_index = 1000000;

-- 4. No false negatives: polygonsWithinSpherical
SELECT
    (SELECT count() FROM t_s2_poly
     WHERE polygonsWithinSpherical(polygon,
        CAST([[(0.0060, 0.0060), (0.0120, 0.0060), (0.0120, 0.0120), (0.0060, 0.0120), (0.0060, 0.0060)]], 'Polygon')))
    =
    (SELECT count() FROM t_s2_poly
     WHERE polygonsWithinSpherical(polygon,
        CAST([[(0.0060, 0.0060), (0.0120, 0.0060), (0.0120, 0.0120), (0.0060, 0.0120), (0.0060, 0.0060)]], 'Polygon'))
     SETTINGS enable_s2_index_pruning = 0);

-- 5. EXPLAIN: projection index in use for polygonsWithinSpherical
EXPLAIN indexes = 1, projections = 1
SELECT id FROM t_s2_poly
WHERE polygonsWithinSpherical(polygon,
    CAST([[(0.0060, 0.0060), (0.0120, 0.0060), (0.0120, 0.0120), (0.0060, 0.0120), (0.0060, 0.0060)]], 'Polygon'))
SETTINGS min_table_rows_to_use_projection_index = 0, max_projection_rows_to_use_projection_index = 1000000;

DROP TABLE t_s2_poly;

-- ── Table 2: MultiPolygon — correctness and EXPLAIN ─────────────────────────────

DROP TABLE IF EXISTS t_s2_multi;
CREATE TABLE t_s2_multi
(
    id UInt64,
    mp MultiPolygon,
    PROJECTION s2_proj INDEX mp TYPE s2(max_cells = 8, min_level = 15, max_level = 15)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_s2_multi VALUES
(
    1,
    CAST(
        [
            [[(0.0000, 0.0000), (0.0100, 0.0000), (0.0100, 0.0100), (0.0000, 0.0100), (0.0000, 0.0000)]],
            [[(5.0000, 5.0000), (5.0100, 5.0000), (5.0100, 5.0100), (5.0000, 5.0100), (5.0000, 5.0000)]]
        ],
        'MultiPolygon')
),
(
    2,
    CAST(
        [
            [[(30.0000, 30.0000), (30.0100, 30.0000), (30.0100, 30.0100), (30.0000, 30.0100), (30.0000, 30.0000)]]
        ],
        'MultiPolygon')
);

-- 4. No false negatives: MultiPolygon
SELECT
    (SELECT count() FROM t_s2_multi
     WHERE polygonsIntersectSpherical(mp,
        CAST([[(0.0010, 0.0010), (0.0090, 0.0010), (0.0090, 0.0090), (0.0010, 0.0090), (0.0010, 0.0010)]], 'Polygon')))
    =
    (SELECT count() FROM t_s2_multi
     WHERE polygonsIntersectSpherical(mp,
        CAST([[(0.0010, 0.0010), (0.0090, 0.0010), (0.0090, 0.0090), (0.0010, 0.0090), (0.0010, 0.0010)]], 'Polygon'))
     SETTINGS enable_s2_index_pruning = 0);

-- 5. EXPLAIN: projection index in use for MultiPolygon
EXPLAIN indexes = 1, projections = 1
SELECT id FROM t_s2_multi
WHERE polygonsIntersectSpherical(mp,
    CAST([[(0.0010, 0.0010), (0.0090, 0.0010), (0.0090, 0.0090), (0.0010, 0.0090), (0.0010, 0.0010)]], 'Polygon'))
SETTINGS min_table_rows_to_use_projection_index = 0, max_projection_rows_to_use_projection_index = 1000000;

DROP TABLE t_s2_multi;

-- ── Table 3: non-strict decode — no crash on non-geometry column ────────────────

DROP TABLE IF EXISTS t_s2_non_strict;
CREATE TABLE t_s2_non_strict
(
    id UInt64,
    s String,
    PROJECTION s2_proj INDEX s TYPE s2(strict_decode = 0)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_s2_non_strict VALUES (1, 'abc');

-- 6. Non-strict decode does not crash, returns correct row count
SELECT count() FROM t_s2_non_strict;

DROP TABLE t_s2_non_strict;

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

-- ── Table 4: Point — S2 projection index on Point column ─────────────────────

DROP TABLE IF EXISTS t_s2_point;
CREATE TABLE t_s2_point
(
    id UInt64,
    location Point,
    PROJECTION s2_proj INDEX location TYPE s2(max_cells = 1, min_level = 16, max_level = 20)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_s2_point VALUES (1, (0.005, 0.005));
INSERT INTO t_s2_point VALUES (2, (0.025, 0.025));
INSERT INTO t_s2_point VALUES (3, (30.0, 30.0));
INSERT INTO t_s2_point VALUES (4, (-30.0, -30.0));
INSERT INTO t_s2_point VALUES (5, (0.015, 0.015));

-- 7. No false negatives: ST_Intersects with Point column
SELECT
    (SELECT count() FROM t_s2_point
     WHERE ST_Intersects(location,
        CAST([[(0.0, 0.0), (0.02, 0.0), (0.02, 0.02), (0.0, 0.02), (0.0, 0.0)]], 'Polygon')))
    =
    (SELECT count() FROM t_s2_point
     WHERE ST_Intersects(location,
        CAST([[(0.0, 0.0), (0.02, 0.0), (0.02, 0.02), (0.0, 0.02), (0.0, 0.0)]], 'Polygon'))
     SETTINGS enable_s2_index_pruning = 0);

-- 8. Correct row IDs returned for Point
SELECT groupArray(id) FROM
(
    SELECT id FROM t_s2_point
    WHERE ST_Intersects(location,
        CAST([[(0.0, 0.0), (0.02, 0.0), (0.02, 0.02), (0.0, 0.02), (0.0, 0.0)]], 'Polygon'))
    ORDER BY id
);

-- 9. No false negatives: ST_DWithin with Point column
SELECT
    (SELECT count() FROM t_s2_point
     WHERE ST_DWithin(location, CAST((0.005, 0.005), 'Point'), 3000))
    =
    (SELECT count() FROM t_s2_point
     WHERE ST_DWithin(location, CAST((0.005, 0.005), 'Point'), 3000)
     SETTINGS enable_s2_index_pruning = 0);

DROP TABLE t_s2_point;

-- ── Table 5: Ring — S2 projection index on Ring column ────────────────────────

DROP TABLE IF EXISTS t_s2_ring;
CREATE TABLE t_s2_ring
(
    id UInt64,
    ring Ring,
    PROJECTION s2_proj INDEX ring TYPE s2(max_cells = 8, min_level = 15, max_level = 15)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_s2_ring VALUES (1, CAST([(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)], 'Ring'));
INSERT INTO t_s2_ring VALUES (2, CAST([(0.02, 0.02), (0.03, 0.02), (0.03, 0.03), (0.02, 0.03), (0.02, 0.02)], 'Ring'));
INSERT INTO t_s2_ring VALUES (3, CAST([(30.0, 30.0), (30.01, 30.0), (30.01, 30.01), (30.0, 30.01), (30.0, 30.0)], 'Ring'));

-- 10. No false negatives: ST_Intersects with Ring column
SELECT
    (SELECT count() FROM t_s2_ring
     WHERE ST_Intersects(ring,
        CAST([[(0.0, 0.0), (0.015, 0.0), (0.015, 0.015), (0.0, 0.015), (0.0, 0.0)]], 'Polygon')))
    =
    (SELECT count() FROM t_s2_ring
     WHERE ST_Intersects(ring,
        CAST([[(0.0, 0.0), (0.015, 0.0), (0.015, 0.015), (0.0, 0.015), (0.0, 0.0)]], 'Polygon'))
     SETTINGS enable_s2_index_pruning = 0);

-- 11. Correct row IDs returned for Ring
SELECT groupArray(id) FROM
(
    SELECT id FROM t_s2_ring
    WHERE ST_Intersects(ring,
        CAST([[(0.0, 0.0), (0.015, 0.0), (0.015, 0.015), (0.0, 0.015), (0.0, 0.0)]], 'Polygon'))
    ORDER BY id
);

DROP TABLE t_s2_ring;

-- ── Table 6: LineString — S2 projection index on LineString column ────────────

DROP TABLE IF EXISTS t_s2_line;
CREATE TABLE t_s2_line
(
    id UInt64,
    line LineString,
    PROJECTION s2_proj INDEX line TYPE s2(max_cells = 8, min_level = 15, max_level = 15)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_s2_line VALUES (1, CAST([(0.0, 0.0), (0.01, 0.01)], 'LineString'));
INSERT INTO t_s2_line VALUES (2, CAST([(0.02, 0.02), (0.03, 0.03)], 'LineString'));
INSERT INTO t_s2_line VALUES (3, CAST([(30.0, 30.0), (30.01, 30.01)], 'LineString'));
INSERT INTO t_s2_line VALUES (4, CAST([(-30.0, -30.0), (-29.99, -29.99)], 'LineString'));

-- 10. No false negatives: ST_Intersects with LineString column
SELECT
    (SELECT count() FROM t_s2_line
     WHERE ST_Intersects(line,
        CAST([[(0.0, 0.0), (0.015, 0.0), (0.015, 0.015), (0.0, 0.015), (0.0, 0.0)]], 'Polygon')))
    =
    (SELECT count() FROM t_s2_line
     WHERE ST_Intersects(line,
        CAST([[(0.0, 0.0), (0.015, 0.0), (0.015, 0.015), (0.0, 0.015), (0.0, 0.0)]], 'Polygon'))
     SETTINGS enable_s2_index_pruning = 0);

-- 11. Correct row IDs returned for LineString
SELECT groupArray(id) FROM
(
    SELECT id FROM t_s2_line
    WHERE ST_Intersects(line,
        CAST([[(0.0, 0.0), (0.015, 0.0), (0.015, 0.015), (0.0, 0.015), (0.0, 0.0)]], 'Polygon'))
    ORDER BY id
);

DROP TABLE t_s2_line;

-- ── Table 6: MultiLineString — S2 projection index on MultiLineString column ─

DROP TABLE IF EXISTS t_s2_mline;
CREATE TABLE t_s2_mline
(
    id UInt64,
    mline MultiLineString,
    PROJECTION s2_proj INDEX mline TYPE s2(max_cells = 8, min_level = 15, max_level = 15)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_s2_mline VALUES
(1, CAST([[(0.0, 0.0), (0.01, 0.01)], [(5.0, 5.0), (5.01, 5.01)]], 'MultiLineString'));
INSERT INTO t_s2_mline VALUES
(2, CAST([[(30.0, 30.0), (30.01, 30.01)]], 'MultiLineString'));

-- 12. No false negatives: ST_Intersects with MultiLineString column
SELECT
    (SELECT count() FROM t_s2_mline
     WHERE ST_Intersects(mline,
        CAST([[(0.0, 0.0), (0.015, 0.0), (0.015, 0.015), (0.0, 0.015), (0.0, 0.0)]], 'Polygon')))
    =
    (SELECT count() FROM t_s2_mline
     WHERE ST_Intersects(mline,
        CAST([[(0.0, 0.0), (0.015, 0.0), (0.015, 0.015), (0.0, 0.015), (0.0, 0.0)]], 'Polygon'))
     SETTINGS enable_s2_index_pruning = 0);

-- 13. Correct row IDs returned for MultiLineString
SELECT groupArray(id) FROM
(
    SELECT id FROM t_s2_mline
    WHERE ST_Intersects(mline,
        CAST([[(0.0, 0.0), (0.015, 0.0), (0.015, 0.015), (0.0, 0.015), (0.0, 0.0)]], 'Polygon'))
    ORDER BY id
);

DROP TABLE t_s2_mline;

-- ── Table 8: Geometry — S2 projection index on Geometry (Variant) column ──────

DROP TABLE IF EXISTS t_s2_geom;
CREATE TABLE t_s2_geom
(
    id UInt64,
    g Geometry,
    PROJECTION s2_proj INDEX g TYPE s2(max_cells = 8, min_level = 15, max_level = 15)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_s2_geom VALUES
(1, CAST((0.005, 0.005), 'Point'));
INSERT INTO t_s2_geom VALUES
(2, CAST([[(10.0, 10.0), (10.01, 10.0), (10.01, 10.01), (10.0, 10.01), (10.0, 10.0)]], 'Polygon'));
INSERT INTO t_s2_geom VALUES
(3, CAST([(30.0, 30.0), (30.01, 30.01)], 'Ring'));
INSERT INTO t_s2_geom VALUES
(4, CAST([(50.0, 50.0), (50.01, 50.01)], 'LineString'));
INSERT INTO t_s2_geom VALUES
(5, CAST([[(70.0, 70.0), (70.01, 70.01)]], 'MultiLineString'));

-- 14. No false negatives: ST_Intersects with Geometry column (mixed types)
SELECT
    (SELECT count() FROM t_s2_geom
     WHERE ST_Intersects(g,
        CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon')))
    =
    (SELECT count() FROM t_s2_geom
     WHERE ST_Intersects(g,
        CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'))
     SETTINGS enable_s2_index_pruning = 0);

-- 15. Correct row IDs returned for Geometry column
SELECT groupArray(id) FROM
(
    SELECT id FROM t_s2_geom
    WHERE ST_Intersects(g,
        CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'))
    ORDER BY id
);

DROP TABLE t_s2_geom;

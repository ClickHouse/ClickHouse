-- Regression test: `spatial_bbox` pruning must fail closed for a `polygonsIntersectCartesian` /
-- `polygonsWithinCartesian` argument whose column geometry kind the predicate rejects.
--
-- Both functions reject `Point`, `LineString`, `MultiPoint` and `MultiLineString` at ANY argument
-- position, but they reject them inside `callOnTwoGeometryDataTypes` while EXECUTING -- their
-- `getReturnTypeImpl` returns `UInt8` without looking at the arguments at all. Nothing evaluates the
-- predicate before granule selection either: `ActionsDAG::updateHeader` only dry-runs a function
-- when every argument is constant, and here one argument is a column.
--
-- So if the skip index prunes every granule, `executeImpl` is never reached on a non-empty block and
-- the query returns `0` instead of raising `ILLEGAL_TYPE_OF_ARGUMENT` -- a wrong answer, not merely a
-- lost optimisation. `extractSpatialPredicateNodeBbox` (`src/Common/GeoBbox.h`) asks
-- `IFunctionBase::rejectsColumnGeometryKind` to fail closed for exactly this case, which both
-- functions must answer.
--
-- Every query below uses a constant polygon disjoint from the single stored granule, so a pruning
-- path that does not fail closed prunes it away and answers `0`.

DROP TABLE IF EXISTS test_spatial_bbox_polygons_point_column;
DROP TABLE IF EXISTS test_spatial_bbox_polygons_linestring_column;
DROP TABLE IF EXISTS test_spatial_bbox_polygons_raw_tuple_column;
DROP TABLE IF EXISTS test_spatial_bbox_polygons_polygon_column;

SET optimize_move_to_prewhere = 0;

-- A `Point` column: rejected at argument 0.
CREATE TABLE test_spatial_bbox_polygons_point_column
(
    id UInt32,
    p  Point,
    INDEX idx_bbox_p p TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO test_spatial_bbox_polygons_point_column VALUES (1, (100., 100.));

SELECT count() FROM test_spatial_bbox_polygons_point_column
WHERE polygonsIntersectCartesian(p, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_polygons_point_column
WHERE polygonsWithinCartesian(p, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A `LineString` column: same representation as `Ring`, told apart only by the custom name, and
-- rejected while `Ring` is accepted.
CREATE TABLE test_spatial_bbox_polygons_linestring_column
(
    id UInt32,
    l  LineString,
    INDEX idx_bbox_l l TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO test_spatial_bbox_polygons_linestring_column VALUES (1, [(100., 100.), (110., 110.)]);

SELECT count() FROM test_spatial_bbox_polygons_linestring_column
WHERE polygonsIntersectCartesian(l, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_polygons_linestring_column
WHERE polygonsWithinCartesian(l, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A bare `Tuple(Float64, Float64)` column carries no custom name to report a kind under, so it is
-- reached through the structural path rather than the named one, and must fail closed just the same:
-- `callOnGeometryDataType` resolves it as a `Point`.
CREATE TABLE test_spatial_bbox_polygons_raw_tuple_column
(
    id UInt32,
    p  Tuple(Float64, Float64),
    INDEX idx_bbox_p p TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO test_spatial_bbox_polygons_raw_tuple_column VALUES (1, (100., 100.));

SELECT count() FROM test_spatial_bbox_polygons_raw_tuple_column
WHERE polygonsIntersectCartesian(p, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Sanity: an accepted kind must still prune. A `Polygon` column is legitimate for both predicates,
-- so the disjoint constant must drop the granule and answer `0` rather than raise.
CREATE TABLE test_spatial_bbox_polygons_polygon_column
(
    id UInt32,
    g  Polygon,
    INDEX idx_bbox_g g TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO test_spatial_bbox_polygons_polygon_column VALUES (1, [[(100., 100.), (110., 100.), (110., 110.), (100., 110.), (100., 100.)]]);

SELECT 'polygon column pruned', count() FROM test_spatial_bbox_polygons_polygon_column
WHERE polygonsIntersectCartesian(g, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]);

SELECT 'polygon column granules', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_polygons_polygon_column
      WHERE polygonsIntersectCartesian(g, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]))
WHERE explain LIKE '%Granules:%';

DROP TABLE test_spatial_bbox_polygons_point_column;
DROP TABLE test_spatial_bbox_polygons_linestring_column;
DROP TABLE test_spatial_bbox_polygons_raw_tuple_column;
DROP TABLE test_spatial_bbox_polygons_polygon_column;

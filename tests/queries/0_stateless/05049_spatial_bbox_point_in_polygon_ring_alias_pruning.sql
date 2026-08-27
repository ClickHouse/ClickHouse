-- Regression test: `pointInPolygon` accepts a polygon-side `LineString`, `MultiPoint` or
-- `MultiLineString` constant, so `spatial_bbox` pruning must stay ON for it.
--
-- `getReturnTypeImpl` checks only the `Array` depth and the innermost `Tuple` of every argument
-- after the first, and `executeImpl` then dispatches on that depth alone -- neither ever looks at
-- the custom name. A `LineString`/`MultiPoint` (both `Array(Tuple(Float64, Float64))`) therefore
-- runs as a `Ring`, and a `MultiLineString` (`Array(Array(Tuple(Float64, Float64)))`) runs as a
-- `Polygon` with holes. `rejectsConstGeometryKind` used to list those three kinds as rejected,
-- which turned pruning off for queries the runtime accepts and, through
-- `rejectsColumnGeometryKindDuringBuild`, told the lenient `Variant`/`Dynamic` adaptors that they
-- collapse to NULL although no type mismatch occurs.
--
-- Failing closed remains right for the kinds `pointInPolygon` really does reject in that position:
-- a `Point` (a `Tuple`, so no `Array` depth at all) and a WKB `String` payload -- see
-- `04904_spatial_bbox_point_in_polygon_first_arg_kind_mismatch`.

DROP TABLE IF EXISTS test_spatial_bbox_pip_ring_alias;

CREATE TABLE test_spatial_bbox_pip_ring_alias
(
    id   UInt32,
    poly Polygon,
    INDEX idx_bbox_poly poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_pip_ring_alias
SELECT number + 1, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]] FROM numbers(4);

SET optimize_move_to_prewhere = 0;

-- A `LineString` constant in the polygon position runs as a `Ring`. The point below is inside it,
-- and the sibling conjunct on the indexed column must still prune its granule away.
SELECT 'linestring const', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_pip_ring_alias
      WHERE pointInPolygon((500.5, 500.5), CAST([(500., 500.), (501., 500.), (501., 501.), (500., 501.)], 'LineString'))
        AND pointInPolygon((900., 900.), poly))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_pip_ring_alias
WHERE pointInPolygon((500.5, 500.5), CAST([(500., 500.), (501., 500.), (501., 501.), (500., 501.)], 'LineString'));

-- The same for a `MultiPoint`, which has the identical representation.
SELECT count() FROM test_spatial_bbox_pip_ring_alias
WHERE pointInPolygon((500.5, 500.5), CAST([(500., 500.), (501., 500.), (501., 501.), (500., 501.)], 'MultiPoint'));

-- And for a `MultiLineString`, which is one `Array` level up and runs as a `Polygon` with holes.
SELECT 'multilinestring const', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_pip_ring_alias
      WHERE pointInPolygon((500.5, 500.5), CAST([[(500., 500.), (501., 500.), (501., 501.), (500., 501.)]], 'MultiLineString'))
        AND pointInPolygon((900., 900.), poly))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_pip_ring_alias
WHERE pointInPolygon((500.5, 500.5), CAST([[(500., 500.), (501., 500.), (501., 501.), (500., 501.)]], 'MultiLineString'));

-- The `Geometry`-typed spellings of the same constants, which reach the predicate through the
-- `Variant` discriminator rather than a named type, must behave identically.
SELECT count() FROM test_spatial_bbox_pip_ring_alias
WHERE pointInPolygon((500.5, 500.5), readWKT('LINESTRING(500 500, 501 500, 501 501, 500 501)'));

SELECT count() FROM test_spatial_bbox_pip_ring_alias
WHERE pointInPolygon((500.5, 500.5), readWKT('MULTILINESTRING((500 500, 501 500, 501 501, 500 501))'));

DROP TABLE test_spatial_bbox_pip_ring_alias;

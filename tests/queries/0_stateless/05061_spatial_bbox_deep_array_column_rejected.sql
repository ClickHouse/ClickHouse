-- Regression test: `spatialBboxIndexValidator` must reject an `Array` nesting deeper than a
-- `MultiPolygon`.
--
-- The validator stripped `Array` wrappers without a depth cap and accepted any column whose ultimate
-- element type is `Tuple(Float64, Float64)`, so `Array(Array(Array(Array(Tuple(Float64, Float64)))))`
-- could carry a `spatial_bbox` index. No spatial predicate can read that shape: `pointInPolygon`
-- rejects `array_depth > 3` in `getReturnTypeImpl`, and `callOnGeometryDataType`
-- (`Functions/geometryConverters.h`) falls through to `Unknown geometry type`. Indexing it anyway
-- lets a granule bbox prune for a column no predicate can consume.
--
-- `Point` is depth 0 and `Ring`/`Polygon`/`MultiPolygon` are one `Array` level apart each, so depth
-- 3 is the deepest accepted shape -- exactly the set the error message names.

DROP TABLE IF EXISTS test_spatial_bbox_deep_array;

CREATE TABLE test_spatial_bbox_deep_array
(
    g Array(Array(Array(Array(Tuple(Float64, Float64))))),
    INDEX idx_bbox_g g TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- A `MultiPolygon`-shaped column spelled structurally is depth 3 and stays accepted.
CREATE TABLE test_spatial_bbox_deep_array
(
    g Array(Array(Array(Tuple(Float64, Float64)))),
    INDEX idx_bbox_g g TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT 'multipolygon depth accepted';

DROP TABLE test_spatial_bbox_deep_array;

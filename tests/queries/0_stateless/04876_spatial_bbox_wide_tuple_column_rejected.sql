-- Regression test: `spatialBboxIndexValidator` (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp)
-- accepted a column whose innermost `Tuple` had TWO OR MORE `Float64` elements, but
-- `addFromGeoColumn` (same file) only ever reads the tuple's first two columns as (x, y) --
-- exactly like `extractBboxFromFieldValue`'s constant-side extractor, which already requires
-- `tuple.size() == 2` and treats a wider tuple as opaque (not a point), for the same reason.
-- A column like `Tuple(Float64, Float64, Float64, Float64)` -- e.g. an (xmin, ymin, xmax, ymax)
-- rect consumed by an `is_spatial_predicate` UDF with entirely different semantics -- would pass
-- the validator, then get silently indexed as a `Point` from just its first two coordinates,
-- discarding the other two: a granule could be pruned even though its rectangle's `xmax`/`ymax`
-- would make the predicate match. This wants the validator to require EXACTLY two elements, so
-- such a column is rejected at index-definition time instead of being silently mis-indexed.

DROP TABLE IF EXISTS test_spatial_bbox_wide_tuple_column_rejected;

CREATE TABLE test_spatial_bbox_wide_tuple_column_rejected
(
    id   UInt32,
    rect Tuple(Float64, Float64, Float64, Float64),
    INDEX idx_bbox rect TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- A plain Point (exactly Tuple(Float64, Float64)) must still be accepted.
CREATE TABLE test_spatial_bbox_wide_tuple_column_rejected
(
    id UInt32,
    p  Point,
    INDEX idx_bbox p TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

DROP TABLE test_spatial_bbox_wide_tuple_column_rejected;

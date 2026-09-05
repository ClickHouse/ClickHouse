-- Regression test: `spatialBboxIndexValidator` (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp) only checked
-- the produced type, not that the index expression is a bare column. `extractQueryBbox` only recognizes
-- `ActionType::INPUT` children matching the indexed column name, so an index on a non-column expression like
-- `tuple(p.1, p.2)` would build and store data but could never be used for pruning. Reject such expressions
-- at index-creation time instead of silently accepting a useless index.

DROP TABLE IF EXISTS test_spatial_bbox_non_column_expr;

CREATE TABLE test_spatial_bbox_non_column_expr
(
    id UInt32,
    p  Point,
    INDEX idx_bbox tuple(p.1, p.2) TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

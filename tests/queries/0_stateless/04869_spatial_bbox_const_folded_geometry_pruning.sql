-- Coverage test: `MergeTreeIndexConditionSpatialBbox::extractNodeBbox` (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp)
-- only recognizes a bare `ActionType::COLUMN` child as constant geometry. A deterministic constant expression
-- like `arrayConcat(poly, [])` reaches it as a `FUNCTION` node -- but `ActionsDAG::removeUnusedActions`'s
-- constant-folding pass (see its `allow_constant_folding` branch) already collapses any node with a
-- materialized `column` into a plain `COLUMN` node before the index condition is built, so `extractNodeBbox`
-- sees the same bare-literal shape either way. This guards that behavior: a constant-folded geometry
-- expression must prune exactly like a bare literal would, protecting against a regression in either
-- `removeUnusedActions`'s folding or `extractNodeBbox`'s literal handling.

DROP TABLE IF EXISTS test_spatial_bbox_const_folded_geometry;

CREATE TABLE test_spatial_bbox_const_folded_geometry
(
    id UInt32,
    p  Point,
    INDEX idx_bbox p TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- First granule: far away, outside the query polygon's bbox. Second granule: inside it.
INSERT INTO test_spatial_bbox_const_folded_geometry SELECT number + 1, (toFloat64(1000 + number), toFloat64(1000 + number)) FROM numbers(4);
INSERT INTO test_spatial_bbox_const_folded_geometry SELECT number + 5, (0.5, 0.5) FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_const_folded_geometry FINAL;

-- `arrayConcat([...], [])` is constant-folded to the same array as a bare literal, so pruning must behave
-- identically to a bare-literal query: only the far-away granule can be skipped.
SELECT pruned_granules < total_granules FROM (
    SELECT
        CAST(splitByChar('/', ratio)[1], 'UInt64') AS pruned_granules,
        CAST(splitByChar('/', ratio)[2], 'UInt64') AS total_granules
    FROM (
        SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS ratio
        FROM (
            SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
            FROM (
                EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_const_folded_geometry
                WHERE pointInPolygon(p, arrayConcat([(0., 0.), (1., 0.), (1., 1.), (0., 1.)], []))
            )
        )
    )
);

-- Sanity check: the query results themselves are correct regardless of pruning.
SELECT count() FROM test_spatial_bbox_const_folded_geometry
WHERE pointInPolygon(p, arrayConcat([(0., 0.), (1., 0.), (1., 1.), (0., 1.)], []));

DROP TABLE test_spatial_bbox_const_folded_geometry;

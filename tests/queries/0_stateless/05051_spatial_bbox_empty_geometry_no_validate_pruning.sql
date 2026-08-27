-- Regression test: with `validate_polygons = 0`, an EMPTY constant geometry must not fail
-- `spatial_bbox` pruning closed.
--
-- `extractBboxFromFieldValue` (`src/Common/GeoBbox.h`) poisoned every empty `Array` -- a top-level
-- `CAST([], 'Ring')` as well as an empty ring nested in `[shell, []]` -- before it ever consulted
-- `require_valid`. That is right by default, where `parseConstPolygon` assembles the same literal
-- and `bg::is_valid` rejects it, so an exception is pending that pruning must not hide. It is wrong
-- with `validate_polygons = 0`: `pointInPolygon` then skips `bg::is_valid`, accepts the empty
-- geometry and simply answers `0` (see `00500_point_in_polygon_empty_bound`), so there is no
-- exception left to preserve and the fail-closed cost buys nothing. The empty piece must instead
-- contribute no information, leaving the surrounding conjunct and its siblings free to prune.

DROP TABLE IF EXISTS test_spatial_bbox_empty_geometry;

CREATE TABLE test_spatial_bbox_empty_geometry
(
    id UInt32,
    p  Point,
    INDEX idx_bbox p TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- Granule 1 (ids 1-4): points inside [0,0]-[1,1].
INSERT INTO test_spatial_bbox_empty_geometry SELECT number + 1, (0.5, 0.5) FROM numbers(4);

-- Granule 2 (ids 5-8): points far away -- must get pruned by the non-empty sibling conjunct.
INSERT INTO test_spatial_bbox_empty_geometry SELECT number + 5, (100., 100.) FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_empty_geometry FINAL;

SET optimize_move_to_prewhere = 0;
SET validate_polygons = 0;

-- A wholly empty constant contributes no bbox, but must not veto the sibling conjunct's pruning:
-- granule 2 lies outside `[(0, 0), (1, 0), (1, 1), (0, 1)]` and must be skipped.
SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_empty_geometry
        WHERE pointInPolygon(p, CAST([], 'Ring'))
          AND pointInPolygon(p, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)])
    )
);

SELECT count() FROM test_spatial_bbox_empty_geometry
WHERE pointInPolygon(p, CAST([], 'Ring'))
  AND pointInPolygon(p, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]);

-- An empty HOLE nested in a polygon literal removes nothing, so the shell's bbox is the whole
-- geometry's bbox and pruning must use it: granule 2 is skipped.
SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_empty_geometry
        WHERE pointInPolygon(p, [[(0., 0.), (1., 0.), (1., 1.), (0., 1.)], []])
    )
);

SELECT count() FROM test_spatial_bbox_empty_geometry
WHERE pointInPolygon(p, [[(0., 0.), (1., 0.), (1., 1.), (0., 1.)], []]);

-- The default `validate_polygons = 1` must keep failing closed for the same literals: there the
-- predicate does raise, and pruning the far granule away would hide it.
SET validate_polygons = 1;

SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_empty_geometry
        WHERE pointInPolygon(p, [[(0., 0.), (1., 0.), (1., 1.), (0., 1.)], []])
    )
);

DROP TABLE test_spatial_bbox_empty_geometry;

-- Regression test: with `validate_polygons = 0`, an EMPTY constant geometry must not fail
-- `spatial_bbox` pruning closed.
--
-- `extractBboxFromFieldValue` (`src/Common/GeoBbox.h`) marked every empty `Array` invalid before it
-- ever consulted `require_valid`. That is right by default, where `parseConstPolygon` assembles the
-- same literal and `bg::is_valid` rejects it, so an exception is pending that pruning must not hide.
-- It is wrong with `validate_polygons = 0`: `pointInPolygon` then skips `bg::is_valid`, accepts the
-- empty geometry and simply answers `0` (see `00500_point_in_polygon_empty_bound`), so there is no
-- exception left to preserve and failing closed only costs pruning -- for the whole surrounding
-- `AND`, not just for the empty conjunct itself.

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

-- The empty constant contributes no bbox of its own, but it must not veto the sibling conjunct's
-- pruning: granule 2 lies outside `[(0, 0), (1, 0), (1, 1), (0, 1)]` and must be skipped.
SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_empty_geometry
        WHERE pointInPolygon(p, CAST([], 'Ring'))
          AND pointInPolygon(p, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)])
        SETTINGS validate_polygons = 0
    )
);

-- Sanity: the query still evaluates, and the empty ring matches nothing.
SELECT count() FROM test_spatial_bbox_empty_geometry
WHERE pointInPolygon(p, CAST([], 'Ring'))
  AND pointInPolygon(p, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)])
SETTINGS validate_polygons = 0;

-- The default `validate_polygons = 1` must keep failing closed for the same constant: there the
-- predicate raises, and pruning the far granule away would turn that exception into a silent `0`.
-- Asserting the exception is the direct statement of the fail-closed property -- an `EXPLAIN` cannot
-- be used here, because the constant is parsed and rejected while the plan's header is computed.
SELECT count() FROM test_spatial_bbox_empty_geometry
WHERE pointInPolygon(p, CAST([], 'Ring'))
  AND pointInPolygon(p, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]); -- { serverError BAD_ARGUMENTS }

DROP TABLE test_spatial_bbox_empty_geometry;

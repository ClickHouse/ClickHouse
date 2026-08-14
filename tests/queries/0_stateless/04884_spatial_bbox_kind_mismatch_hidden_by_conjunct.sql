-- Regression test: `extractSpatialPredicateNodeBbox` (src/Common/GeoBbox.h) used to treat a
-- constant explicitly typed as a geometry kind the calling predicate is guaranteed to reject
-- (e.g. a `Point`/`LineString`/`MultiPoint`/`MultiLineString` constant passed to
-- `polygonsIntersectCartesian`/`polygonsWithinCartesian`, which only accept
-- `Ring`/`Polygon`/`MultiPolygon`) the same as "not geometry-shaped" -- `NodeBboxStatus::NoInfo`
-- -- rather than `NodeBboxStatus::Failed`. 04882/04883 already cover this for a *standalone*
-- predicate, where `NoInfo` alone disables pruning for that predicate and the exception still
-- surfaces. But inside an `AND` conjunction, a sibling conjunct with a genuinely disjoint bbox
-- could still prune every granule using ONLY its own bbox, discarding the granule before the
-- guaranteed-to-raise conjunct ever runs on real data.
--
-- This can't be observed end-to-end through `serverError` alone: `polygonsIntersectCartesian`'s
-- kind check lives inside `executeImpl` and only fires when it is actually called with at least
-- one row, and when every real row fails the first conjunct, the executor's own `AND` masking
-- already skips calling the second conjunct at all -- independent of whether the index pruned
-- anything. So the fix must instead be verified at the plan level, via `EXPLAIN indexes=1`: the
-- `spatial_bbox` skip index must not appear (no pruning) whenever a sibling conjunct is
-- guaranteed to raise, exactly as it would if the index didn't exist at all.

DROP TABLE IF EXISTS test_spatial_bbox_kind_mismatch_hidden_by_conjunct;

CREATE TABLE test_spatial_bbox_kind_mismatch_hidden_by_conjunct
(
    id   UInt32,
    poly Polygon,
    INDEX idx_bbox_poly poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_kind_mismatch_hidden_by_conjunct
SELECT number + 1, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]] FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_kind_mismatch_hidden_by_conjunct FINAL;

-- The first conjunct's constant polygon is disjoint from every row's bbox, so it alone would
-- prune the single granule -- but the second conjunct is guaranteed to raise, so pruning must be
-- disabled for the whole conjunction: no Skip section, and every granule must still be read.
SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS skip_granules,
       extract(explain_text, '(?s)ReadFromMergeTree.*?Granules: ([0-9]+)') AS read_granules
FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_kind_mismatch_hidden_by_conjunct
        WHERE polygonsIntersectCartesian(poly, [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]])
          AND polygonsIntersectCartesian(poly, readWKT('POINT(0 0)'))
        SETTINGS short_circuit_function_evaluation = 'disable'
    )
);

SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS skip_granules,
       extract(explain_text, '(?s)ReadFromMergeTree.*?Granules: ([0-9]+)') AS read_granules
FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_kind_mismatch_hidden_by_conjunct
        WHERE polygonsWithinCartesian(poly, [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]])
          AND polygonsWithinCartesian(poly, readWKT('POINT(0 0)'))
        SETTINGS short_circuit_function_evaluation = 'disable'
    )
);

-- Same gap for the LineString/MultiPoint/MultiLineString kinds that 04883 covers standalone.
SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS skip_granules,
       extract(explain_text, '(?s)ReadFromMergeTree.*?Granules: ([0-9]+)') AS read_granules
FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_kind_mismatch_hidden_by_conjunct
        WHERE polygonsIntersectCartesian(poly, [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]])
          AND polygonsIntersectCartesian(poly, readWKT('LINESTRING(0 0,1 1)'))
        SETTINGS short_circuit_function_evaluation = 'disable'
    )
);

SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS skip_granules,
       extract(explain_text, '(?s)ReadFromMergeTree.*?Granules: ([0-9]+)') AS read_granules
FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_kind_mismatch_hidden_by_conjunct
        WHERE polygonsIntersectCartesian(poly, [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]])
          AND polygonsIntersectCartesian(poly, readWKT('MULTIPOINT(0 0,1 1)'))
        SETTINGS short_circuit_function_evaluation = 'disable'
    )
);

SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS skip_granules,
       extract(explain_text, '(?s)ReadFromMergeTree.*?Granules: ([0-9]+)') AS read_granules
FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_kind_mismatch_hidden_by_conjunct
        WHERE polygonsIntersectCartesian(poly, [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]])
          AND polygonsIntersectCartesian(poly, readWKT('MULTILINESTRING((0 0,1 1),(2 2,3 3))'))
        SETTINGS short_circuit_function_evaluation = 'disable'
    )
);

-- Sanity: two genuinely valid conjuncts, with the first still disjoint from every row, must
-- still prune via the index (Skip section present, every granule discarded).
SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS skip_granules,
       extract(explain_text, '(?s)ReadFromMergeTree.*?Granules: ([0-9]+)') AS read_granules
FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_kind_mismatch_hidden_by_conjunct
        WHERE polygonsIntersectCartesian(poly, [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]])
          AND polygonsIntersectCartesian(poly, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]])
        SETTINGS short_circuit_function_evaluation = 'disable'
    )
);

-- Sanity: two genuinely valid, non-disjoint conjuncts must still evaluate correctly end-to-end
-- (no regression from the `Failed`-status propagation).
SELECT count() FROM test_spatial_bbox_kind_mismatch_hidden_by_conjunct
WHERE polygonsIntersectCartesian(poly, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]])
  AND polygonsIntersectCartesian(poly, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]])
SETTINGS short_circuit_function_evaluation = 'disable';

-- Sanity: end-to-end, with at least one real row surviving the first conjunct, the guaranteed-
-- to-raise second conjunct must actually raise (confirms this isn't just a plan-level artifact).
DROP TABLE IF EXISTS test_spatial_bbox_kind_mismatch_hidden_by_conjunct_mixed;

CREATE TABLE test_spatial_bbox_kind_mismatch_hidden_by_conjunct_mixed
(
    id   UInt32,
    poly Polygon,
    INDEX idx_bbox_poly poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_kind_mismatch_hidden_by_conjunct_mixed
SELECT number + 1, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]] FROM numbers(4);

INSERT INTO test_spatial_bbox_kind_mismatch_hidden_by_conjunct_mixed
SELECT number + 5, [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]] FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_kind_mismatch_hidden_by_conjunct_mixed FINAL;

SELECT count() FROM test_spatial_bbox_kind_mismatch_hidden_by_conjunct_mixed
WHERE polygonsIntersectCartesian(poly, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]])
  AND polygonsIntersectCartesian(poly, readWKT('POINT(0 0)'))
SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE test_spatial_bbox_kind_mismatch_hidden_by_conjunct_mixed;

DROP TABLE test_spatial_bbox_kind_mismatch_hidden_by_conjunct;

-- A `Nullable` argument makes a geometry function's `Unknown geometry type` exception disappear when
-- a sibling conjunct filters the block empty: `defaultImplementationForNulls` returns an empty result
-- for `input_rows_count == 0` before `executeImpl` runs, so `callOnTwoGeometryDataTypes` never gets to
-- raise. That is a trunk defect, tracked in https://github.com/ClickHouse/ClickHouse/issues/117208.
--
-- What the `spatial_bbox` index owes is only that it does not CAUSE it: a granule bbox is a
-- conservative superset, so it can prune only granules the sibling conjunct would have filtered
-- anyway. Each shape below is therefore run twice, with the index enabled and disabled, and the two
-- answers must agree -- whatever they are. A control table carrying no index at all shows the same
-- answer a third time.

DROP TABLE IF EXISTS test_spatial_bbox_nullable_arg;
DROP TABLE IF EXISTS test_spatial_bbox_nullable_arg_no_index;

CREATE TABLE test_spatial_bbox_nullable_arg
(
    poly Polygon,
    n Nullable(UInt8),
    INDEX idx_bbox poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

CREATE TABLE test_spatial_bbox_nullable_arg_no_index
(
    poly Polygon,
    n Nullable(UInt8)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO test_spatial_bbox_nullable_arg VALUES ([[(100., 100.), (101., 100.), (101., 101.), (100., 100.)]], 1);
INSERT INTO test_spatial_bbox_nullable_arg_no_index VALUES ([[(100., 100.), (101., 100.), (101., 101.), (100., 100.)]], 1);

-- A sibling `Nullable(UInt8)` column.
SELECT 'nullable sibling column', count() FROM test_spatial_bbox_nullable_arg
WHERE pointInPolygon((0., 0.), poly) AND polygonsIntersectCartesian(n, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0, use_skip_indexes = 1;

SELECT 'nullable sibling column', count() FROM test_spatial_bbox_nullable_arg
WHERE pointInPolygon((0., 0.), poly) AND polygonsIntersectCartesian(n, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0, use_skip_indexes = 0;

SELECT 'nullable sibling column', count() FROM test_spatial_bbox_nullable_arg_no_index
WHERE pointInPolygon((0., 0.), poly) AND polygonsIntersectCartesian(n, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0;

-- A `Nullable` non-geometry constant in an otherwise well-typed call.
SELECT 'nullable constant', count() FROM test_spatial_bbox_nullable_arg
WHERE pointInPolygon((0., 0.), poly) AND polygonsIntersectCartesian(poly, CAST(1 AS Nullable(UInt8)))
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0, use_skip_indexes = 1;

SELECT 'nullable constant', count() FROM test_spatial_bbox_nullable_arg
WHERE pointInPolygon((0., 0.), poly) AND polygonsIntersectCartesian(poly, CAST(1 AS Nullable(UInt8)))
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0, use_skip_indexes = 0;

SELECT 'nullable constant', count() FROM test_spatial_bbox_nullable_arg_no_index
WHERE pointInPolygon((0., 0.), poly) AND polygonsIntersectCartesian(poly, CAST(1 AS Nullable(UInt8)))
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0;

-- Without the `Nullable` wrapper the exception surfaces with the index active, as it must.
SELECT count() FROM test_spatial_bbox_nullable_arg
WHERE pointInPolygon((0., 0.), poly) AND polygonsIntersectCartesian(poly, 1)
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError BAD_ARGUMENTS }

DROP TABLE test_spatial_bbox_nullable_arg_no_index;
DROP TABLE test_spatial_bbox_nullable_arg;

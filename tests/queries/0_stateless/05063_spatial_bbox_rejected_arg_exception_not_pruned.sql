-- A `spatial_bbox` index must never turn an exception into a `0`. Every shape below has a sibling
-- conjunct whose argument the predicate rejects, next to an indexed conjunct whose bbox prunes the
-- only granule away. The rejection comes from `callOnTwoGeometryDataTypes`, which dispatches on
-- argument TYPES and raises before reading a row, so it fires even on the empty block that pruning
-- leaves behind -- pruning costs nothing but speed here. These queries pin that.

DROP TABLE IF EXISTS test_spatial_bbox_rejected_arg;

CREATE TABLE test_spatial_bbox_rejected_arg
(
    poly Polygon,
    p Point,
    ls LineString,
    s String,
    named Tuple(x Float64, y Float64),
    INDEX idx_bbox poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO test_spatial_bbox_rejected_arg VALUES ([[(100., 100.), (101., 100.), (101., 101.), (100., 100.)]], (100., 100.), [(100., 100.), (101., 101.)], 'x', (100., 100.));

-- A non-geometry constant argument.
SELECT count() FROM test_spatial_bbox_rejected_arg
WHERE pointInPolygon((0., 0.), poly) AND polygonsIntersectCartesian(poly, 1)
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError BAD_ARGUMENTS }

-- A non-geometry sibling COLUMN.
SELECT count() FROM test_spatial_bbox_rejected_arg
WHERE pointInPolygon((0., 0.), poly) AND polygonsIntersectCartesian(s, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError BAD_ARGUMENTS }

-- A sibling column of a geometry kind the predicate refuses at this argument position.
SELECT count() FROM test_spatial_bbox_rejected_arg
WHERE pointInPolygon((0., 0.), poly) AND polygonsIntersectCartesian(p, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_rejected_arg
WHERE pointInPolygon((0., 0.), poly) AND polygonsIntersectCartesian(ls, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- An explicit-name tuple: `DataTypeTuple::equals` compares element names, so this is not a `Point`
-- and `callOnGeometryDataType` cannot resolve it.
SELECT count() FROM test_spatial_bbox_rejected_arg
WHERE pointInPolygon((0., 0.), poly) AND polygonsIntersectCartesian(named, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError BAD_ARGUMENTS }

-- The same explicit-name tuple as the INDEXED column of its own table, where the bbox is built from
-- it directly rather than from a sibling.
DROP TABLE IF EXISTS test_spatial_bbox_rejected_arg_named;

CREATE TABLE test_spatial_bbox_rejected_arg_named
(
    named Tuple(x Float64, y Float64),
    INDEX idx_bbox named TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO test_spatial_bbox_rejected_arg_named VALUES ((100., 100.));

SELECT count() FROM test_spatial_bbox_rejected_arg_named
WHERE polygonsIntersectCartesian(named, [[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]); -- { serverError BAD_ARGUMENTS }

DROP TABLE test_spatial_bbox_rejected_arg_named;
DROP TABLE test_spatial_bbox_rejected_arg;

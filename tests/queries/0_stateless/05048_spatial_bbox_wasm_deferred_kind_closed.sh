#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Regression test: `hasDeferredGeometryKindRejection` (`src/Common/GeoBbox.h`) decides whether a
# deferred `Dynamic`/`Variant` geometry argument can raise by asking the predicate's
# `rejectsColumnGeometryKind`. `FunctionUserDefinedWasm` used to leave that hook at its default
# `false`, even though its `getReturnTypeImpl` raises `ILLEGAL_TYPE_OF_ARGUMENT` for every argument
# whose type is not the declared one. A strict `is_spatial_predicate = 1` UDF therefore looked
# incapable of raising on kind grounds: a sibling conjunct on the indexed column pruned every
# granule, and on the resulting `0`-row block `ExecutableFunctionVariantAdaptor` returns an empty
# result without ever building the rejecting overload -- so the query answered `0` instead of
# surfacing the type mismatch.
#
# The hook is now derived from the UDF's declared argument types, so it is precise rather than
# blanket: the `Geometry` column below (whose alternatives include kinds the UDF rejects) fails
# closed, while a `Variant` whose only alternative IS the declared kind keeps pruning.
#
# It is derived as a REPRESENTATION, not a name. `getReturnTypeImpl` accepts any argument whose type
# `equals` the declared one, and `DataTypeArray::equals`/`DataTypeTuple::equals` ignore the outer
# custom name, so a UDF declared on `Ring` runs on a `LineString`/`MultiPoint` column and one
# declared on `Polygon` runs on a `MultiLineString` column. Comparing names alone would report those
# as rejected and disable pruning for queries that cannot raise.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_point_in_rect_deferred;
DROP FUNCTION IF EXISTS wasm_ring_pred_deferred;
DROP FUNCTION IF EXISTS wasm_polygon_pred_deferred;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_deferred';
DROP TABLE IF EXISTS test_spatial_bbox_wasm_deferred;
EOF

cat "$CUR_DIR/wasm/spatial_predicate.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'spatial_predicate_deferred', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION wasm_point_in_rect_deferred
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_deferred' :: 'point_in_rect'
    ARGUMENTS (geom Point, rect Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'RowBinary', is_spatial_predicate = 1;

CREATE FUNCTION wasm_ring_pred_deferred
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_deferred' :: 'point_in_rect'
    ARGUMENTS (g Ring, q Ring) RETURNS UInt8
    SETTINGS serialization_format = 'RowBinary', is_spatial_predicate = 1;

CREATE FUNCTION wasm_polygon_pred_deferred
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_deferred' :: 'point_in_rect'
    ARGUMENTS (g Polygon, q Polygon) RETURNS UInt8
    SETTINGS serialization_format = 'RowBinary', is_spatial_predicate = 1;

CREATE TABLE test_spatial_bbox_wasm_deferred
(
    a Polygon,
    b Geometry,
    c Variant(Tuple(Float64, Float64)),
    d Variant(LineString),
    e Variant(MultiLineString),
    INDEX idx_bbox_a a TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

-- The indexed polygon sits near (100, 100), far from the (0, 0) point every query below tests, so
-- the sibling conjunct on \`a\` prunes the only granule unless the UDF conjunct fails closed.
-- \`b\` stores a \`LineString\`, a kind the UDF's declared \`Point\` argument rejects.
INSERT INTO test_spatial_bbox_wasm_deferred
SELECT [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]],
       CAST([(0., 0.), (1., 1.)], 'LineString')::Geometry,
       CAST((0., 0.), 'Tuple(Float64, Float64)'),
       CAST([(0., 0.), (1., 1.)], 'LineString'),
       CAST([[(0., 0.), (1., 1.)]], 'MultiLineString')
FROM numbers(4);

SET short_circuit_function_evaluation = 'disable';
SET optimize_move_to_prewhere = 0;

-- \`b\` is a \`Geometry\` (a \`Variant\` over every geometry kind), so which overload runs is settled
-- per row. \`LineString\`, \`Polygon\`, ... are all rejected by the declared \`Point\` argument, so
-- pruning must stay off and the query must raise.
SELECT 'wasm deferred geometry closed', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_deferred
      WHERE wasm_point_in_rect_deferred(b, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)])
        AND pointInPolygon((0., 0.), a))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_wasm_deferred
WHERE wasm_point_in_rect_deferred(b, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)])
  AND pointInPolygon((0., 0.), a); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- \`c\`'s only alternative is an unnamed \`Tuple(Float64, Float64)\`, which is structurally the
-- declared \`Point\`, so no per-row overload can raise and pruning must stay on.
SELECT 'wasm deferred point open', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_deferred
      WHERE wasm_point_in_rect_deferred(c, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)])
        AND pointInPolygon((0., 0.), a))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_wasm_deferred
WHERE wasm_point_in_rect_deferred(c, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)])
  AND pointInPolygon((0., 0.), a);

-- \`LineString\` and the declared \`Ring\` are the same type to \`equals\` -- both
-- \`Array(Tuple(Float64, Float64))\` -- so this UDF runs on \`d\` and cannot raise on kind grounds:
-- pruning must stay on.
SELECT 'wasm ring accepts linestring', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_deferred
      WHERE wasm_ring_pred_deferred(d, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)])
        AND pointInPolygon((0., 0.), a))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_wasm_deferred
WHERE wasm_ring_pred_deferred(d, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)])
  AND pointInPolygon((0., 0.), a);

-- The same one \`Array\` level up: \`MultiLineString\` and the declared \`Polygon\` are both
-- \`Array(Array(Tuple(Float64, Float64)))\`.
SELECT 'wasm polygon accepts multilinestring', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_deferred
      WHERE wasm_polygon_pred_deferred(e, [[(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]])
        AND pointInPolygon((0., 0.), a))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_wasm_deferred
WHERE wasm_polygon_pred_deferred(e, [[(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]])
  AND pointInPolygon((0., 0.), a);

DROP TABLE test_spatial_bbox_wasm_deferred;
DROP FUNCTION wasm_polygon_pred_deferred;
DROP FUNCTION wasm_ring_pred_deferred;
DROP FUNCTION wasm_point_in_rect_deferred;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_deferred';
EOF

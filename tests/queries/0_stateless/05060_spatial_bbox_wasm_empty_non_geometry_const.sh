#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Regression test: an EMPTY constant argument that is no geometry at all must not veto pruning.
#
# `extractBboxFromFieldValue` (`src/Common/GeoBbox.h`) interprets a constant argument by the SHAPE
# of its `Field`, and treats an empty `Array` as an invalid geometry -- `CAST([], 'Ring')` is the
# same value `parseConstPolygon` rejects at execute time, so it must fail closed. But the `Field` of
# `CAST([], 'Array(Int32)')` has exactly that shape too, and for an `is_spatial_predicate = 1` WASM
# UDF declaring such an auxiliary argument, `FunctionUserDefinedWasm::getReturnTypeImpl` accepts it
# and execution never raises. Failing closed there costs pruning for a query that cannot raise.
#
# The declared type tells the two apart, so a constant whose type is no geometry at all is now
# skipped before its `Field` is interpreted by shape. Skipping cannot hide an exception: a native
# predicate handed a non-geometry argument raises from `callOnTwoGeometryDataTypes`, which dispatches
# on the argument TYPES and so raises even on a fully pruned, zero-row block.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_spatial_empty_aux;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_empty_aux';
DROP TABLE IF EXISTS test_spatial_bbox_wasm_empty_aux;
EOF

cat "$CUR_DIR/wasm/spatial_predicate.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'spatial_predicate_empty_aux', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION wasm_spatial_empty_aux
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_empty_aux' :: 'always_true'
    ARGUMENTS (geom Point, poly Array(Tuple(Float64, Float64)), aux Array(Int32)) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

CREATE TABLE test_spatial_bbox_wasm_empty_aux
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- First granule far from the origin, second granule at (0.5, 0.5) inside the constant ring below.
INSERT INTO test_spatial_bbox_wasm_empty_aux SELECT number + 1, (toFloat64(1000 + number), toFloat64(1000 + number)) FROM numbers(4);
INSERT INTO test_spatial_bbox_wasm_empty_aux SELECT number + 5, (0.5, 0.5) FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_wasm_empty_aux FINAL;

SET short_circuit_function_evaluation = 'disable';

-- The empty \`Array(Int32)\` is an auxiliary argument the UDF accepts, so the constant ring's bbox
-- must still prune the far-away granule away.
SELECT 'empty aux array', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_empty_aux
      WHERE wasm_spatial_empty_aux(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)], CAST([], 'Array(Int32)')))
WHERE explain LIKE '%Granules:%';

SELECT 'empty aux array', count() FROM test_spatial_bbox_wasm_empty_aux
WHERE wasm_spatial_empty_aux(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)], CAST([], 'Array(Int32)'));

-- An EMPTY GEOMETRY constant still fails closed: this is the \`CAST([], 'Ring')\` case the shape
-- check exists for, and the UDF's declared \`poly\` argument is a geometry.
SELECT 'empty ring const', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_empty_aux
      WHERE wasm_spatial_empty_aux(geom, CAST([], 'Array(Tuple(Float64, Float64))'), CAST([1], 'Array(Int32)')))
WHERE explain LIKE '%Granules:%';

DROP TABLE test_spatial_bbox_wasm_empty_aux;
DROP FUNCTION wasm_spatial_empty_aux;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_empty_aux';
EOF

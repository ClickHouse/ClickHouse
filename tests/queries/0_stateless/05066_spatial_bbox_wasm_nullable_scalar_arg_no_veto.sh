#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# A `Nullable` argument the predicate REJECTS must fail bbox pruning closed: with
# `useDefaultImplementationForNulls`, `IFunction::execute` returns an empty result for
# `input_rows_count == 0` before the nested function is built, so the type dispatch that would
# have raised never runs and the exception becomes a silent `0`. That is what
# `05064_spatial_bbox_nullable_arg_index_neutral` pins.
#
# This test pins the other side of that rule: a `Nullable` argument the predicate ACCEPTS raises
# nothing, so it must keep its pruning. An `is_spatial_predicate` WASM UDF declaring a plain
# scalar auxiliary argument accepts a `Nullable` constant there -- `getReturnTypeImpl` sees the
# type with the `Nullable` already stripped -- and a sibling `pointInPolygon` conjunct must still
# be free to prune.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'SETUP'
DROP FUNCTION IF EXISTS wasm_spatial2_nullable_scalar_no_veto;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_nullable_scalar';
DROP TABLE IF EXISTS test_spatial_bbox_wasm_nullable_scalar;
SETUP

cat "$CUR_DIR/wasm/spatial_predicate.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'spatial_predicate_nullable_scalar', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'QUERIES'
-- Ignores every argument, always returns 1: only the SHAPE of the arguments matters here.
CREATE FUNCTION wasm_spatial2_nullable_scalar_no_veto
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_nullable_scalar' :: 'always_true'
    ARGUMENTS (geom Point, poly_const Array(Tuple(Float64, Float64)), threshold Int32) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

CREATE TABLE test_spatial_bbox_wasm_nullable_scalar
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- First granule: far away, outside the first conjunct's polygon. Second granule: inside it.
INSERT INTO test_spatial_bbox_wasm_nullable_scalar SELECT number + 1, (toFloat64(1000 + number), toFloat64(1000 + number)) FROM numbers(4);
INSERT INTO test_spatial_bbox_wasm_nullable_scalar SELECT number + 5, (0.5, 0.5) FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_wasm_nullable_scalar FINAL;

-- The declared `Int32` threshold accepts a `Nullable(Int32)` constant, so nothing is left to
-- raise and the sibling `pointInPolygon` conjunct must still prune.
SELECT if(ratio = '', 0, CAST(splitByChar('/', ratio)[1], 'UInt64') < CAST(splitByChar('/', ratio)[2], 'UInt64')) FROM (
    SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS ratio
    FROM (
        SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
        FROM (
            EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_nullable_scalar
            WHERE pointInPolygon(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)])
              AND wasm_spatial2_nullable_scalar_no_veto(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)], CAST(100 AS Nullable(Int32)))
            SETTINGS short_circuit_function_evaluation = 'disable'
        )
    )
);

DROP TABLE test_spatial_bbox_wasm_nullable_scalar;
DROP FUNCTION wasm_spatial2_nullable_scalar_no_veto;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_nullable_scalar';
QUERIES

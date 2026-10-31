#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Regression test: `extractSpatialPredicateNodeBbox` (src/Common/GeoBbox.h) used
# `!extractBboxFromFieldValue(field, field_acc)` alone to decide a constant argument is
# guaranteed to raise on evaluation and must veto pruning for the whole `and`. But
# `extractBboxFromFieldValue`'s `false` return is overloaded between two different cases:
# a geometry-shaped argument that failed validation (which DOES poison `field_acc.valid`), and
# a constant that isn't geometry-shaped at all -- e.g. a plain scalar argument to an
# `is_spatial_predicate` WASM UDF, such as a distance threshold (which leaves `field_acc.valid`
# untouched, still `true`). Only the former is guaranteed to raise; the latter is simply not
# evaluated as geometry and executes normally. Treating both as `Failed` disabled pruning for
# the WHOLE conjunction -- including a sibling, individually-valid `pointInPolygon` conjunct --
# any time a spatial-predicate UDF took a harmless non-geometry constant argument.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_spatial2_scalar_only_no_veto;
DROP FUNCTION IF EXISTS wasm_spatial2_poly_and_scalar_no_veto;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_scalar_no_veto';
DROP TABLE IF EXISTS test_spatial_bbox_wasm_scalar_arg_no_veto;
EOF

cat "$CUR_DIR/wasm/spatial_predicate.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'spatial_predicate_scalar_no_veto', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
-- Ignores every argument, always returns 1: only the SHAPE of the predicate's arguments
-- (constant scalar vs. constant geometry) matters for this test, not its actual semantics.
CREATE FUNCTION wasm_spatial2_scalar_only_no_veto
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_scalar_no_veto' :: 'always_true'
    ARGUMENTS (geom Point, threshold Int32) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

CREATE FUNCTION wasm_spatial2_poly_and_scalar_no_veto
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_scalar_no_veto' :: 'always_true'
    ARGUMENTS (geom Point, poly_const Array(Tuple(Float64, Float64)), threshold Int32) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

CREATE TABLE test_spatial_bbox_wasm_scalar_arg_no_veto
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- First granule: far away, outside the first conjunct's polygon. Second granule: inside it.
INSERT INTO test_spatial_bbox_wasm_scalar_arg_no_veto SELECT number + 1, (toFloat64(1000 + number), toFloat64(1000 + number)) FROM numbers(4);
INSERT INTO test_spatial_bbox_wasm_scalar_arg_no_veto SELECT number + 5, (0.5, 0.5) FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_wasm_scalar_arg_no_veto FINAL;

-- A lone scalar constant argument (not geometry-shaped at all) must not veto pruning driven by
-- the sibling pointInPolygon conjunct.
SELECT if(ratio = '', 0, CAST(splitByChar('/', ratio)[1], 'UInt64') < CAST(splitByChar('/', ratio)[2], 'UInt64')) FROM (
    SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS ratio
    FROM (
        SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
        FROM (
            EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_scalar_arg_no_veto
            WHERE pointInPolygon(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)])
              AND wasm_spatial2_scalar_only_no_veto(geom, 100)
            SETTINGS short_circuit_function_evaluation = 'disable'
        )
    )
);

-- A valid constant geometry argument alongside a scalar constant argument (two or more constant
-- arguments on a non-pointInPolygon predicate) must likewise not veto pruning just because the
-- scalar isn't geometry-shaped.
SELECT if(ratio = '', 0, CAST(splitByChar('/', ratio)[1], 'UInt64') < CAST(splitByChar('/', ratio)[2], 'UInt64')) FROM (
    SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS ratio
    FROM (
        SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
        FROM (
            EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_scalar_arg_no_veto
            WHERE pointInPolygon(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)])
              AND wasm_spatial2_poly_and_scalar_no_veto(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)], 100)
            SETTINGS short_circuit_function_evaluation = 'disable'
        )
    )
);

DROP TABLE test_spatial_bbox_wasm_scalar_arg_no_veto;
DROP FUNCTION wasm_spatial2_scalar_only_no_veto;
DROP FUNCTION wasm_spatial2_poly_and_scalar_no_veto;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_scalar_no_veto';
EOF

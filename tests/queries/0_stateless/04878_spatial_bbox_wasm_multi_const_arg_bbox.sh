#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Regression test: `extractSpatialPredicateNodeBbox` (src/Common/GeoBbox.h) gave up on deriving
# a bbox for ANY `isSpatialPredicate()` UDF call with two or more constant arguments, as long as
# the function wasn't the builtin `pointInPolygon` -- even when exactly one of those constants
# was actually geometry-shaped and the rest were auxiliary scalars. A common signature like
# `f(geom_column, const_poly, threshold)` (e.g. a distance-threshold predicate) therefore never
# got any `spatial_bbox` pruning at all, contradicting the documented contract in `wasm_udf.mdx`
# that `f(geom_column, const_geom, ...)` should be prunable. Fixed by reducing this case to the
# already-trusted single-constant-geometry bbox whenever exactly one constant argument is
# geometry-shaped. A `String` auxiliary constant remains ambiguous with WKB and is intentionally
# NOT covered by this relaxation (see `wasm_udf.mdx`): it must still fail closed if unparseable,
# since silently ignoring it could hide a genuine "invalid WKB" exception.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_spatial2_poly_and_threshold;
DROP FUNCTION IF EXISTS wasm_spatial2_poly_and_string;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_multi_const_arg';
DROP TABLE IF EXISTS test_spatial_bbox_wasm_multi_const_arg_bbox;
EOF

cat "$CUR_DIR/wasm/spatial_predicate.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'spatial_predicate_multi_const_arg', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
-- Ignores every argument, always returns 1: only the SHAPE of the predicate's arguments
-- (constant geometry vs. constant scalar) matters for this test, not its actual semantics.
CREATE FUNCTION wasm_spatial2_poly_and_threshold
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_multi_const_arg' :: 'always_true'
    ARGUMENTS (geom Point, poly_const Array(Tuple(Float64, Float64)), threshold Int32) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

CREATE FUNCTION wasm_spatial2_poly_and_string
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_multi_const_arg' :: 'always_true'
    ARGUMENTS (geom Point, poly_const Array(Tuple(Float64, Float64)), mode String) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

CREATE TABLE test_spatial_bbox_wasm_multi_const_arg_bbox
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- First granule: far away, outside the polygon. Second granule: inside it.
INSERT INTO test_spatial_bbox_wasm_multi_const_arg_bbox SELECT number + 1, (toFloat64(1000 + number), toFloat64(1000 + number)) FROM numbers(4);
INSERT INTO test_spatial_bbox_wasm_multi_const_arg_bbox SELECT number + 5, (0.5, 0.5) FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_wasm_multi_const_arg_bbox FINAL;

-- A UDF taking exactly one constant geometry argument plus an auxiliary scalar, used ALONE
-- (no sibling pointInPolygon conjunct to derive a bbox from), must now be pruned using the
-- geometry constant's own bbox.
SELECT if(ratio = '', 0, CAST(splitByChar('/', ratio)[1], 'UInt64') < CAST(splitByChar('/', ratio)[2], 'UInt64')) FROM (
    SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS ratio
    FROM (
        SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
        FROM (
            EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_multi_const_arg_bbox
            WHERE wasm_spatial2_poly_and_threshold(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)], 100)
            SETTINGS short_circuit_function_evaluation = 'disable'
        )
    )
);

-- A String auxiliary constant remains ambiguous with WKB: an unparseable one must still fail
-- closed (no pruning at all -- but a correct, non-empty result), not silently derive a bbox from
-- just the polygon argument while ignoring it.
SELECT count() FROM test_spatial_bbox_wasm_multi_const_arg_bbox
WHERE wasm_spatial2_poly_and_string(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)], 'not-wkb')
SETTINGS short_circuit_function_evaluation = 'disable';

SELECT if(ratio = '', 1, 0) FROM (
    SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS ratio
    FROM (
        SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
        FROM (
            EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_multi_const_arg_bbox
            WHERE wasm_spatial2_poly_and_string(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)], 'not-wkb')
            SETTINGS short_circuit_function_evaluation = 'disable'
        )
    )
);

DROP TABLE test_spatial_bbox_wasm_multi_const_arg_bbox;
DROP FUNCTION wasm_spatial2_poly_and_threshold;
DROP FUNCTION wasm_spatial2_poly_and_string;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_multi_const_arg';
EOF

#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Regression test: `extractSpatialPredicateNodeBbox` (src/Common/GeoBbox.h) checked
# `const_fields.size() > 1 && function name != pointInPolygon` and returned `NoInfo`
# unconditionally, before ever validating those individual constant geometry arguments --
# unlike the single-constant-argument case just below it, which already vetoes pruning
# (`Failed`) when its one constant argument fails to extract/validate. For a variadic
# `isSpatialPredicate()` function with two or more constant geometry arguments that aren't
# assembled into one combined shape (only `pointInPolygon` does that), an invalid argument
# (self-intersecting / not `bg::is_valid`) was therefore silently downgraded to `NoInfo`
# -- contributing no pruning information, but also never forcing `extractQueryBbox` to fail
# closed -- instead of `Failed`. Combined via `and` with another, valid conjunct under
# `short_circuit_function_evaluation = 'disable'`, that valid conjunct's bbox alone could
# then drive pruning while the invalid argument was never even inspected, defeating the
# fail-closed contract `is_spatial_predicate = 1` UDFs are documented to rely on.
#
# This can't be observed via row results with the harmless `always_true` test UDF (it
# doesn't validate its geometry arguments, so results are correct either way): pruning
# using only the first, valid conjunct's bbox is always sound for row correctness on its
# own. The bug is only observable in *how much* pruning happens -- so this asserts on
# `EXPLAIN indexes = 1`'s granule ratio: an invalid constant argument on the second
# conjunct must disable pruning entirely (every granule scanned), not merely fail to
# contribute a bbox.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_spatial2_always_true_multi_invalid;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_multi_const_arg_invalid';
DROP TABLE IF EXISTS test_spatial_bbox_wasm_multi_const_arg_invalid;
EOF

cat "$CUR_DIR/wasm/spatial_predicate.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'spatial_predicate_multi_const_arg_invalid', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION wasm_spatial2_always_true_multi_invalid
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_multi_const_arg_invalid' :: 'always_true'
    ARGUMENTS (geom Point, poly1 Array(Tuple(Float64, Float64)), poly2 Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

CREATE TABLE test_spatial_bbox_wasm_multi_const_arg_invalid
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- First granule: far away, outside the first conjunct's polygon. Second granule: inside it.
INSERT INTO test_spatial_bbox_wasm_multi_const_arg_invalid SELECT number + 1, (toFloat64(1000 + number), toFloat64(1000 + number)) FROM numbers(4);
INSERT INTO test_spatial_bbox_wasm_multi_const_arg_invalid SELECT number + 5, (0.5, 0.5) FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_wasm_multi_const_arg_invalid FINAL;

-- Second conjunct has two constant geometry arguments: \`poly1\` is a valid ring, \`poly2\` is a
-- self-intersecting (bowtie) ring -- not \`bg::is_valid\`. Since the UDF is not \`pointInPolygon\`,
-- these two arguments are never assembled together, but each is still guaranteed to be evaluated,
-- and the invalid one is still guaranteed to raise. The first conjunct's bbox alone would prune the
-- far-away granule; that must not happen once the second conjunct's invalid constant argument is
-- taken into account: pruning must be disabled entirely (fail closed). When the whole query's bbox
-- extraction fails closed, \`idx_bbox\` isn't applied at all and so doesn't even appear in
-- \`EXPLAIN indexes = 1\`'s output (no \`ratio\` match) -- that also counts as "not pruned" here,
-- alongside the case where it appears with a 1:1 (unpruned) ratio.
SELECT if(ratio = '', 0, CAST(splitByChar('/', ratio)[1], 'UInt64') < CAST(splitByChar('/', ratio)[2], 'UInt64')) FROM (
    SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS ratio
    FROM (
        SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
        FROM (
            EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_multi_const_arg_invalid
            WHERE pointInPolygon(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)])
              AND wasm_spatial2_always_true_multi_invalid(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)], [(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)])
            SETTINGS short_circuit_function_evaluation = 'disable'
        )
    )
);

DROP TABLE test_spatial_bbox_wasm_multi_const_arg_invalid;
DROP FUNCTION wasm_spatial2_always_true_multi_invalid;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_multi_const_arg_invalid';
EOF

#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Regression test: `MergeTreeIndexConditionSpatialBbox::extractNodeBbox` (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp)
# checked `has_extra_non_constant` and returned `NoInfo` before ever validating the predicate's
# constant geometry argument(s) -- unlike the `!input_child` path just above it, which already
# vetoes pruning (`Failed`) when a constant argument fails to extract. For a variadic
# `isSpatialPredicate()` function with more than one non-constant input, e.g. a WASM UDF
# `f(geom, other_id, const_poly)` (see 04840_spatial_bbox_wasm_multi_input.sh), an invalid
# `const_poly` (self-intersecting / not `bg::is_valid`) was therefore silently downgraded to
# `NoInfo` -- contributing no pruning information, but also never forcing `extractQueryBbox` to
# fail closed -- instead of `Failed`. Combined via `and` with another, valid conjunct under
# `short_circuit_function_evaluation = 'disable'`, that valid conjunct's bbox alone could then
# drive pruning while the invalid argument was never even inspected, defeating the fail-closed
# contract `is_spatial_predicate = 1` UDFs are documented to rely on.
#
# This can't be observed via row results with the harmless `always_true`/`point_in_rect` test
# UDFs (neither validates its geometry argument, so results are correct either way): pruning
# using only the first, valid conjunct's bbox is always sound for row correctness on its own.
# The bug is only observable in *how much* pruning happens -- so this asserts on
# `EXPLAIN indexes = 1`'s granule ratio: an invalid constant argument on the second conjunct
# must disable pruning entirely (every granule scanned), not merely fail to contribute a bbox.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_spatial2_always_true_extra_arg;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_extra_arg_invalid';
DROP TABLE IF EXISTS test_spatial_bbox_wasm_extra_arg_invalid_geometry;
EOF

cat "$CUR_DIR/wasm/spatial_predicate.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'spatial_predicate_extra_arg_invalid', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION wasm_spatial2_always_true_extra_arg
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_extra_arg_invalid' :: 'always_true'
    ARGUMENTS (geom Point, other_id UInt32, poly_const Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

CREATE TABLE test_spatial_bbox_wasm_extra_arg_invalid_geometry
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- First granule: far away, outside the first conjunct's polygon. Second granule: inside it.
INSERT INTO test_spatial_bbox_wasm_extra_arg_invalid_geometry SELECT number + 1, (toFloat64(1000 + number), toFloat64(1000 + number)) FROM numbers(4);
INSERT INTO test_spatial_bbox_wasm_extra_arg_invalid_geometry SELECT number + 5, (0.5, 0.5) FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_wasm_extra_arg_invalid_geometry FINAL;

-- Second conjunct's constant polygon argument is a self-intersecting (bowtie) ring -- not
-- \`bg::is_valid\` -- combined with the extra non-constant \`id\` argument. The first conjunct's
-- bbox alone would prune the far-away granule; that must not happen once the second conjunct's
-- invalid constant argument is taken into account: pruning must be disabled entirely (fail
-- closed). When the whole query's bbox extraction fails closed, \`idx_bbox\` isn't applied at all
-- and so doesn't even appear in \`EXPLAIN indexes = 1\`'s output (no \`ratio\` match) -- that also
-- counts as "not pruned" here, alongside the case where it appears with a 1:1 (unpruned) ratio.
SELECT if(ratio = '', 0, CAST(splitByChar('/', ratio)[1], 'UInt64') < CAST(splitByChar('/', ratio)[2], 'UInt64')) FROM (
    SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS ratio
    FROM (
        SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
        FROM (
            EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_extra_arg_invalid_geometry
            WHERE pointInPolygon(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)])
              AND wasm_spatial2_always_true_extra_arg(geom, id, [(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)])
            SETTINGS short_circuit_function_evaluation = 'disable'
        )
    )
);

DROP TABLE test_spatial_bbox_wasm_extra_arg_invalid_geometry;
DROP FUNCTION wasm_spatial2_always_true_extra_arg;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_extra_arg_invalid';
EOF

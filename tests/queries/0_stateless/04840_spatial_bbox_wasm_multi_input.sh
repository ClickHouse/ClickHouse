#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Regression test for the fail-closed multi-input guard in
# `MergeTreeIndexConditionSpatialBbox::extractQueryBbox` (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp).
# A WASM UDF opted into pruning via `is_spatial_predicate = 1` can have a signature
# with more than one non-constant input, e.g. `f(geom, id, const_poly)`. Deriving a
# query bbox from just the indexed `geom` column while ignoring that `id` also
# participates would be unsafe: the predicate could be true because of `id` (or the
# UDF could ignore `geom` entirely), and pruning on `geom`'s bbox alone would
# incorrectly skip matching rows. The guard must disable pruning entirely for such
# predicates rather than pruning on a partial view of the arguments.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_spatial2_always_true_mt;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_multi_input_mt';
DROP TABLE IF EXISTS test_spatial_bbox_wasm_multi_input;
EOF

cat "$CUR_DIR/wasm/spatial_predicate.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'spatial_predicate_multi_input_mt', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION wasm_spatial2_always_true_mt
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_multi_input_mt' :: 'always_true'
    ARGUMENTS (geom Point, other_id UInt32, poly_const Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

CREATE TABLE test_spatial_bbox_wasm_multi_input
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- All points far away from the constant polygon argument below.
INSERT INTO test_spatial_bbox_wasm_multi_input SELECT
    number + 1 AS id,
    (toFloat64((number % 4) + 100), toFloat64((number % 4) + 100)) AS geom
FROM numbers(8);

OPTIMIZE TABLE test_spatial_bbox_wasm_multi_input FINAL;

-- Multi-input spatial predicate: must return all 8 rows (the UDF is unconditionally
-- true), proving the fail-closed guard does not silently prune based on \`geom\` alone.
SELECT count() FROM test_spatial_bbox_wasm_multi_input
WHERE wasm_spatial2_always_true_mt(geom, id, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]);

DROP TABLE test_spatial_bbox_wasm_multi_input;
DROP FUNCTION wasm_spatial2_always_true_mt;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_multi_input_mt';
EOF

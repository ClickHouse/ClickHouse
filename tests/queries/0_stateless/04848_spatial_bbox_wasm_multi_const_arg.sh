#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Regression test: `MergeTreeIndexConditionSpatialBbox::extractNodeBbox` (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp)
# used to assemble two-or-more constant geometry arguments of ANY `isSpatialPredicate()` function
# into one combined Polygon-with-holes / MultiPolygon, mirroring `FunctionPointInPolygon`'s
# `is_const_multi_polygon` semantics. That assembly is specific to `pointInPolygon` -- a different
# `isSpatialPredicate()` function (e.g. a WASM UDF) with more than one constant geometry argument
# could combine them under entirely different semantics, so applying pointInPolygon's shell/hole
# assembly to it produces a bogus bbox. Here the UDF ignores its geometry arguments and always
# returns true; the first constant argument (a ring far from every row) would wrongly become the
# "shell" that all rows must fall inside, and every granule would be incorrectly pruned.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_spatial2_always_true_ma;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_multi_const_arg_ma';
DROP TABLE IF EXISTS test_spatial_bbox_wasm_multi_const_arg;
EOF

cat "$CUR_DIR/wasm/spatial_predicate.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'spatial_predicate_multi_const_arg_ma', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION wasm_spatial2_always_true_ma
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_multi_const_arg_ma' :: 'always_true'
    ARGUMENTS (geom Point, poly1 Array(Tuple(Float64, Float64)), poly2 Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

CREATE TABLE test_spatial_bbox_wasm_multi_const_arg
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- All points far away from both constant polygon arguments below.
INSERT INTO test_spatial_bbox_wasm_multi_const_arg SELECT
    number + 1 AS id,
    (toFloat64((number % 4) + 1000), toFloat64((number % 4) + 1000)) AS geom
FROM numbers(8);

OPTIMIZE TABLE test_spatial_bbox_wasm_multi_const_arg FINAL;

-- The UDF is unconditionally true regardless of \`geom\` or the constant polygons: must return
-- all 8 rows, proving the multi-const-arg assembly path isn't applied to a non-pointInPolygon
-- spatial predicate (which would incorrectly prune every granule as disjoint from poly1's ring).
SELECT count() FROM test_spatial_bbox_wasm_multi_const_arg
WHERE wasm_spatial2_always_true_ma(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)], [(50., 50.), (51., 50.), (51., 51.), (50., 51.), (50., 50.)]);

DROP TABLE test_spatial_bbox_wasm_multi_const_arg;
DROP FUNCTION wasm_spatial2_always_true_ma;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_multi_const_arg_ma';
EOF

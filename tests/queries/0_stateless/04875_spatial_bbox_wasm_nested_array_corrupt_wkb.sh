#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Regression test: `extractBboxFromFieldValue`'s generic `Array` fallback (src/Common/GeoBbox.h,
# the loop after the ring/polygon/multipolygon-shaped array checks) recurses into each element and
# returns `acc.found`, discarding each recursive call's own return value. The `String` (WKB) branch's
# `catch (...)` block returned `false` on a parse failure without poisoning `acc.valid`, relying on
# callers checking the return value directly -- which works for a single top-level WKB argument, but
# not when it's nested inside an array: once an earlier array element sets `acc.found = true`, a
# later corrupt WKB element's discarded `false` return leaves `acc.found` (and `acc.valid`) as if
# nothing had gone wrong. A constant `Array(String)` argument like `[valid_wkb, corrupt_wkb]` was
# therefore still treated as `Ok`, using only the valid element's partial bbox, instead of failing
# the whole conjunct closed the way a single corrupt WKB argument already does (see
# 04874_spatial_bbox_wasm_wkb_invalid_geometry).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_spatial2_always_true_nested_array;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_nested_array_invalid';
DROP TABLE IF EXISTS test_spatial_bbox_wasm_nested_array_corrupt_wkb;
EOF

cat "$CUR_DIR/wasm/spatial_predicate.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'spatial_predicate_nested_array_invalid', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION wasm_spatial2_always_true_nested_array
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_nested_array_invalid' :: 'always_true'
    ARGUMENTS (geom Point, wkb_list Array(String)) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

CREATE TABLE test_spatial_bbox_wasm_nested_array_corrupt_wkb
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- First granule: far away, outside the first conjunct's polygon. Second granule: inside it.
INSERT INTO test_spatial_bbox_wasm_nested_array_corrupt_wkb SELECT number + 1, (toFloat64(1000 + number), toFloat64(1000 + number)) FROM numbers(4);
INSERT INTO test_spatial_bbox_wasm_nested_array_corrupt_wkb SELECT number + 5, (0.5, 0.5) FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_wasm_nested_array_corrupt_wkb FINAL;

-- Second conjunct's constant argument is an Array(String) of two WKB blobs: a valid unit-square
-- polygon followed by a corrupt, unparseable one. The first conjunct's bbox alone would prune the
-- far-away granule; that must not happen once the second conjunct's array argument -- with its
-- corrupt element nested past an already-valid one -- is taken into account: pruning must be
-- disabled entirely (fail closed), the same as for a single top-level corrupt WKB argument.
SELECT if(ratio = '', 0, CAST(splitByChar('/', ratio)[1], 'UInt64') < CAST(splitByChar('/', ratio)[2], 'UInt64')) FROM (
    SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS ratio
    FROM (
        SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
        FROM (
            EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_nested_array_corrupt_wkb
            WHERE pointInPolygon(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)])
              AND wasm_spatial2_always_true_nested_array(geom, [wkb(CAST([[(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]], 'Polygon')), 'not-a-valid-wkb-blob'])
            SETTINGS short_circuit_function_evaluation = 'disable'
        )
    )
);

DROP TABLE test_spatial_bbox_wasm_nested_array_corrupt_wkb;
DROP FUNCTION wasm_spatial2_always_true_nested_array;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_nested_array_invalid';
EOF

#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Regression test: `extractBboxFromFieldValue`'s WKB (`String`) branch (src/Common/GeoBbox.h)
# parsed a WKB-encoded `Polygon`/`MultiPolygon` and accumulated its bbox without ever calling
# `boost::geometry::is_valid` on it -- unlike the Array-literal ring/polygon/multipolygon
# branches just above it, which validate via `bg::correct` + `bg::is_valid` and fail extraction
# (fail closed) for a self-intersecting shape. A WASM UDF that is an `isSpatialPredicate()` and
# takes a constant `String` (WKB) geometry argument -- built-in `pointInPolygon` itself only
# accepts the array-of-tuples literal syntax, not WKB -- could therefore have an invalid WKB
# polygon silently treated as a valid, extractable bbox, allowing a sibling conjunct's bbox to
# drive pruning instead of vetoing it, defeating the fail-closed contract
# `is_spatial_predicate = 1` UDFs are documented to rely on.
#
# This can't be observed via row results with the harmless `always_true` test UDF (it doesn't
# validate its geometry argument, so results are correct either way): pruning using only the
# first, valid conjunct's bbox is always sound for row correctness on its own. The bug is only
# observable in *how much* pruning happens -- so this asserts on `EXPLAIN indexes = 1`'s granule
# ratio: an invalid constant WKB argument on the second conjunct must disable pruning entirely
# (every granule scanned), not merely fail to contribute a bbox.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_spatial2_always_true_wkb;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_wkb_invalid';
DROP TABLE IF EXISTS test_spatial_bbox_wasm_wkb_invalid_geometry;
EOF

cat "$CUR_DIR/wasm/spatial_predicate.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'spatial_predicate_wkb_invalid', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION wasm_spatial2_always_true_wkb
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_wkb_invalid' :: 'always_true'
    ARGUMENTS (geom Point, poly_wkb String) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

CREATE TABLE test_spatial_bbox_wasm_wkb_invalid_geometry
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- First granule: far away, outside the first conjunct's polygon. Second granule: inside it.
INSERT INTO test_spatial_bbox_wasm_wkb_invalid_geometry SELECT number + 1, (toFloat64(1000 + number), toFloat64(1000 + number)) FROM numbers(4);
INSERT INTO test_spatial_bbox_wasm_wkb_invalid_geometry SELECT number + 5, (0.5, 0.5) FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_wasm_wkb_invalid_geometry FINAL;

-- Second conjunct's constant argument is a WKB-encoded self-intersecting (bowtie) polygon --
-- not \`bg::is_valid\` -- built via \`wkb(CAST(..., 'Polygon'))\`, folded to a constant at query
-- analysis time. The first conjunct's bbox alone would prune the far-away granule; that must not
-- happen once the second conjunct's invalid constant WKB argument is taken into account: pruning
-- must be disabled entirely (fail closed). When the whole query's bbox extraction fails closed,
-- \`idx_bbox\` isn't applied at all and so doesn't even appear in \`EXPLAIN indexes = 1\`'s output
-- (no \`ratio\` match) -- that also counts as "not pruned" here, alongside the case where it
-- appears with a 1:1 (unpruned) ratio.
SELECT if(ratio = '', 0, CAST(splitByChar('/', ratio)[1], 'UInt64') < CAST(splitByChar('/', ratio)[2], 'UInt64')) FROM (
    SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') AS ratio
    FROM (
        SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
        FROM (
            EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_wkb_invalid_geometry
            WHERE pointInPolygon(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)])
              AND wasm_spatial2_always_true_wkb(geom, wkb(CAST([[(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)]], 'Polygon')))
            SETTINGS short_circuit_function_evaluation = 'disable'
        )
    )
);

DROP TABLE test_spatial_bbox_wasm_wkb_invalid_geometry;
DROP FUNCTION wasm_spatial2_always_true_wkb;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_wkb_invalid';
EOF

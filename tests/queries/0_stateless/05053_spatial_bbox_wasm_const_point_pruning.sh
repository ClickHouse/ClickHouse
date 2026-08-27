#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Regression test: a constant at an `is_spatial_predicate` WASM UDF argument DECLARED `Point` must
# yield a bbox.
#
# `tryExtractConstGeoField` (`src/Common/GeoBbox.h`) flattens both `CAST((0., 0.) AS Point)` and a
# bare `(0., 0.)` literal to the same `Tuple(Float64, Float64)`-shaped `Field`, and
# `extractBboxFromFieldValue` treats that shape as opaque unless the predicate opts in through
# `treatsConstTupleAsPoint` -- none of `pointInPolygon`'s polygon-component arguments, nor
# `polygonsIntersectCartesian`'s, accept a bare point there. `FunctionUserDefinedWasm` never
# overrode the hook, so a UDF declared `(geom Point, rect Ring)` came back as
# `NodeBboxStatus::NoInfo` and lost pruning on its indexed `rect` column for every query the runtime
# accepts perfectly well. The hook is now answered from the declared representation, which also
# covers a `Variant`/`Dynamic` constant, since the adaptors forward it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << SQL
DROP FUNCTION IF EXISTS wasm_point_in_rect_const;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_const_point';
DROP TABLE IF EXISTS test_spatial_bbox_wasm_const_point;
SQL

cat "$CUR_DIR/wasm/spatial_predicate.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'spatial_predicate_const_point', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << SQL
CREATE FUNCTION wasm_point_in_rect_const
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_const_point' :: 'point_in_rect'
    ARGUMENTS (geom Point, rect Ring) RETURNS UInt8
    SETTINGS serialization_format = 'RowBinary', is_spatial_predicate = 1;

CREATE TABLE test_spatial_bbox_wasm_const_point
(
    rect Ring,
    INDEX idx_bbox_rect rect TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

-- The only granule sits near (100, 100), far from the (0, 0) constant point every query below
-- passes, so each of them must prune it away.
INSERT INTO test_spatial_bbox_wasm_const_point
SELECT [(100., 100.), (110., 100.), (110., 110.), (100., 110.)] FROM numbers(4);

SET short_circuit_function_evaluation = 'disable';
SET optimize_move_to_prewhere = 0;

-- An explicitly \`Point\`-typed constant.
SELECT 'named point const', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_const_point
      WHERE wasm_point_in_rect_const(CAST((0., 0.), 'Point'), rect))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_wasm_const_point
WHERE wasm_point_in_rect_const(CAST((0., 0.), 'Point'), rect);

-- A bare tuple literal, which \`getReturnTypeImpl\` accepts because \`equals\` ignores custom names.
SELECT 'bare tuple const', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_const_point
      WHERE wasm_point_in_rect_const((0., 0.), rect))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_wasm_const_point
WHERE wasm_point_in_rect_const((0., 0.), rect);

-- The same value behind a \`Dynamic\`, which the adaptor forwards to the same hook.
SELECT 'dynamic point const', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_const_point
      WHERE wasm_point_in_rect_const(CAST((0., 0.), 'Tuple(Float64, Float64)')::Dynamic, rect))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_wasm_const_point
WHERE wasm_point_in_rect_const(CAST((0., 0.), 'Tuple(Float64, Float64)')::Dynamic, rect);

DROP TABLE test_spatial_bbox_wasm_const_point;
DROP FUNCTION wasm_point_in_rect_const;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_const_point';
SQL

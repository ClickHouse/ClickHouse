#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Reference test: `spatial_bbox` bbox pruning is keyed off `isSpatialPredicate()`, not off
# `pointInPolygon` specifically (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp). This
# uses a genuine (if trivial) non-pointInPolygon spatial predicate -- axis-aligned rectangle
# containment, i.e. the same relation as the `bbox_op_rcontains` bbox shortcut behind `st_within`
# in a real GEOS-backed WASM UDF geometry library -- to show pruning also works for a predicate
# whose semantics have nothing to do with `pointInPolygon`'s ring/hole assembly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
DROP FUNCTION IF EXISTS wasm_point_in_rect;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_real';
DROP TABLE IF EXISTS test_spatial_bbox_wasm_real_predicate;
EOF

cat "$CUR_DIR/wasm/spatial_predicate.wasm" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'spatial_predicate_real', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF
CREATE FUNCTION wasm_point_in_rect
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_real' :: 'point_in_rect'
    ARGUMENTS (geom Point, rect Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'RowBinary', is_spatial_predicate = 1;

CREATE TABLE test_spatial_bbox_wasm_real_predicate
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_wasm_real_predicate SELECT
    number + 1 AS id,
    (toFloat64(number), toFloat64(number)) AS geom
FROM numbers(16);

OPTIMIZE TABLE test_spatial_bbox_wasm_real_predicate FINAL;

-- Rectangle [2, 5] x [2, 5]: only points (2,2)..(5,5) qualify -- ids 3..6.
SELECT count() FROM test_spatial_bbox_wasm_real_predicate
WHERE wasm_point_in_rect(geom, [(2., 2.), (5., 2.), (5., 5.), (2., 5.), (2., 2.)]);

-- Rectangle entirely outside the data's bbox: proves pruning via the constant rect's bbox
-- actually happens for this non-pointInPolygon predicate, not just that the result is correct.
-- The EXPLAIN confirms the spatial_bbox index is consulted and prunes all granules.
SELECT count() FROM test_spatial_bbox_wasm_real_predicate
WHERE wasm_point_in_rect(geom, [(100., 100.), (200., 100.), (200., 200.), (100., 200.), (100., 100.)]);

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_wasm_real_predicate
    WHERE wasm_point_in_rect(geom, [(100., 100.), (200., 100.), (200., 200.), (100., 200.), (100., 100.)])
) WHERE explain LIKE '%Name: idx_bbox%' OR explain LIKE '%Granules: 0/%';

DROP TABLE test_spatial_bbox_wasm_real_predicate;
DROP FUNCTION wasm_point_in_rect;
DELETE FROM system.webassembly_modules WHERE name = 'spatial_predicate_real';
EOF

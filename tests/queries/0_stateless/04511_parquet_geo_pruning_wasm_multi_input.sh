#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
#
# Regression test for the fail-closed multi-input guard in
# `tryExtractSpatialFilterFromNode` (src/Processors/Formats/Impl/Parquet/GeoFilter.cpp).
# A WASM UDF opted into pruning via `is_spatial_predicate = 1` can have a signature
# with more than one non-constant geometry-like input, e.g. `f(left_geom, right_id,
# const_poly)`. Picking just the first such input's bbox for pruning would be unsafe:
# the predicate could be true because of the *other* input, and pruning on only one
# input's bbox would incorrectly skip matching rows. The guard must disable pruning
# entirely for such predicates rather than guessing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="$CUR_DIR/data_parquet/04059_geo_spatial_pruning.parquet"
WASM_CONFIG="$CUR_DIR/../../config/config.d/wasm_udf.xml"

$CLICKHOUSE_LOCAL --config-file "$WASM_CONFIG" --queries-file /dev/stdin <<EOF
INSERT INTO system.webassembly_modules (name, code)
SELECT 'spatial_predicate_multi_input', raw_blob FROM file('$CUR_DIR/wasm/spatial_predicate.wasm', RawBLOB);

CREATE FUNCTION wasm_spatial2_always_true
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_multi_input' :: 'always_true'
    ARGUMENTS (geom Point, other_id Int32, poly_const Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

-- Multi-input spatial predicate: must return all 4 rows (the UDF is unconditionally
-- true), proving the fail-closed guard does not silently prune based on \`geom\` alone.
SELECT '=== all rows returned (fail-closed, no incorrect pruning) ===';
SELECT id FROM file('$FILE', Parquet)
WHERE wasm_spatial2_always_true(geometry, id, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id;
EOF

# No row group should be reported as pruned for this multi-input predicate.
echo "=== pruned_row_groups (expect no pruning) ==="
multi_pruned=$($CLICKHOUSE_LOCAL --config-file "$WASM_CONFIG" --print-profile-events --queries-file /dev/stdin <<EOF 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //'
INSERT INTO system.webassembly_modules (name, code)
SELECT 'spatial_predicate_multi_input', raw_blob FROM file('$CUR_DIR/wasm/spatial_predicate.wasm', RawBLOB);

CREATE FUNCTION wasm_spatial2_always_true
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_multi_input' :: 'always_true'
    ARGUMENTS (geom Point, other_id Int32, poly_const Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

SELECT id FROM file('$FILE', Parquet)
WHERE wasm_spatial2_always_true(geometry, id, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id;
EOF
)
echo "${multi_pruned:-no pruning}"

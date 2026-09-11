#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Positive coverage for the `LANGUAGE WASM` UDF path of `Parquet`/GeoParquet row-group pruning
# (`extractSpatialFilters` in `src/Processors/Formats/Impl/Parquet/GeoFilter.cpp`).
#
# The rest of the UDF-on-`Parquet` coverage (`04511`, `04871`, `04872`) is all fail-closed: it
# proves the reader does NOT prune for multi-input predicates or invalid constants. All of it would
# still pass if UDF extraction on this path regressed to "never produce a spatial filter at all".
# `04059_parquet_geo_spatial_pruning.sh` proves the builtin
# path and `04853_spatial_bbox_wasm_real_predicate.sh` proves the `MergeTree` UDF path; this test
# proves that a well-behaved single-input `is_spatial_predicate = 1` UDF actually prunes row groups
# on `Parquet`.
#
# The UDF ignores its geometry arguments and always returns true, so every row it sees is returned:
# the row count alone reports which row groups survived pruning, and `ParquetPrunedRowGroups` reports
# it directly. The fixture has two row groups, ids 1-2 in south Texas and ids 3-4 in north Texas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="$CUR_DIR/data_parquet/04059_geo_spatial_pruning.parquet"
WASM_CONFIG="$CUR_DIR/../../config/config.d/wasm_udf.xml"

# $1: the constant ring passed to the UDF, $2: extra flags for clickhouse-local
run_query() {
    $CLICKHOUSE_LOCAL --config-file "$WASM_CONFIG" $2 --queries-file /dev/stdin <<EOF
INSERT INTO system.webassembly_modules (name, code)
SELECT 'spatial_predicate_positive_parquet', raw_blob FROM file('$CUR_DIR/wasm/spatial_predicate.wasm', RawBLOB);

CREATE FUNCTION wasm_spatial_always_true_parquet
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_positive_parquet' :: 'always_true'
    ARGUMENTS (geom Point, poly Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

SELECT id FROM file('$FILE', Parquet)
WHERE wasm_spatial_always_true_parquet(geometry, $1)
ORDER BY id;
EOF
}

SOUTH="[(-99., 29.), (-96., 29.), (-96., 31.), (-99., 31.), (-99., 29.)]"
ATLANTIC="[(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]"

# The south Texas ring is disjoint from the north Texas row group, which must be pruned away.
echo "=== south texas (expect ids 1, 2) ==="
run_query "$SOUTH"
echo "=== south texas pruning count ==="
run_query "$SOUTH" --print-profile-events 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //'

# A ring far from every row must prune BOTH row groups, even though the UDF itself always returns
# true -- the pruning decision is made from the constant's bbox alone.
echo "=== atlantic (expect no rows) ==="
run_query "$ATLANTIC"
echo "=== atlantic pruning count ==="
run_query "$ATLANTIC" --print-profile-events 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //'

# With push-down disabled nothing is pruned and the UDF sees every row.
echo "=== atlantic, push-down disabled (expect all 4 rows) ==="
$CLICKHOUSE_LOCAL --config-file "$WASM_CONFIG" --queries-file /dev/stdin <<EOF
INSERT INTO system.webassembly_modules (name, code)
SELECT 'spatial_predicate_positive_parquet', raw_blob FROM file('$CUR_DIR/wasm/spatial_predicate.wasm', RawBLOB);

CREATE FUNCTION wasm_spatial_always_true_parquet
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_positive_parquet' :: 'always_true'
    ARGUMENTS (geom Point, poly Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

SELECT id FROM file('$FILE', Parquet)
WHERE wasm_spatial_always_true_parquet(geometry, $ATLANTIC)
ORDER BY id
SETTINGS input_format_parquet_spatial_filter_push_down = 0;
EOF

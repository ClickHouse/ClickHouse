#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Regression test: `extractSpatialFilters` (src/Processors/Formats/Impl/Parquet/GeoFilter.cpp), via the
# shared `extractSpatialPredicateNodeBbox` template (src/Common/GeoBbox.h), used to combine the constant
# geometry arguments of ANY `isSpatialPredicate()` function with 2+ such arguments into one bbox by
# UNIONING them independently -- mirroring `pointInPolygon`'s shell/hole assembly regardless of which
# function it actually was. That assembly is specific to `pointInPolygon`: a different
# `isSpatialPredicate()` function (e.g. a WASM UDF) with more than one constant geometry argument could
# combine them under entirely different semantics, so applying `pointInPolygon`'s union to it produces a
# bogus bbox -- exactly the bug already fixed for `MergeTree`'s `spatial_bbox` index in `extractNodeBbox`
# (see 04848_spatial_bbox_wasm_multi_const_arg.sh). Here the UDF ignores its geometry arguments and
# always returns true; the two constant rings are both far from every row in the fixture, so the old
# per-argument union bbox is disjoint from both `Parquet` row groups and would incorrectly prune every
# row, silently returning zero rows instead of all four.
#
# A direct `pointInPolygon`-based test cannot observe this: ClickHouse evaluates `WHERE`-clause functions
# once on a 0-row block before any storage read or row-group pruning happens, so an invalid constant
# polygon argument to `pointInPolygon` itself always raises `BAD_ARGUMENTS` immediately regardless of
# whether the underlying pruning logic is buggy or fixed -- the exception fires before pruning code ever
# runs. A WASM UDF that ignores its geometry arguments never raises that exception, making the actual
# pruning decision (and thus the wrong-result bug) observable via the query's row count.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="$CUR_DIR/data_parquet/04059_geo_spatial_pruning.parquet"
WASM_CONFIG="$CUR_DIR/../../config/config.d/wasm_udf.xml"

# poly1 is a ring near the origin, poly2 a ring near (50, 50) -- both far from every row's real Texas
# coordinates (~(-99..-96, 29..35)). The UDF ignores geom/poly1/poly2 entirely and always returns true,
# so the correct result is all 4 rows regardless of pruning.
echo "=== all 4 rows (expect no pruning to silently drop any) ==="
$CLICKHOUSE_LOCAL --config-file "$WASM_CONFIG" --queries-file /dev/stdin <<EOF
INSERT INTO system.webassembly_modules (name, code)
SELECT 'spatial_predicate_multi_arg_dispatch', raw_blob FROM file('$CUR_DIR/wasm/spatial_predicate.wasm', RawBLOB);

CREATE FUNCTION wasm_spatial2_always_true_multiarg
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_multi_arg_dispatch' :: 'always_true'
    ARGUMENTS (geom Point, poly1 Array(Tuple(Float64, Float64)), poly2 Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

SELECT id FROM file('$FILE', Parquet)
WHERE wasm_spatial2_always_true_multiarg(geometry, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)], [(50., 50.), (51., 50.), (51., 51.), (50., 51.), (50., 50.)])
ORDER BY id;
EOF

# No row group should be reported as pruned: the multi-const-arg assembly path is specific to
# `pointInPolygon`, so this UDF's two constant rings must not be combined into a bbox at all.
echo "=== pruned_row_groups (expect no pruning) ==="
pruned=$($CLICKHOUSE_LOCAL --config-file "$WASM_CONFIG" --print-profile-events --queries-file /dev/stdin <<EOF 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //'
INSERT INTO system.webassembly_modules (name, code)
SELECT 'spatial_predicate_multi_arg_dispatch', raw_blob FROM file('$CUR_DIR/wasm/spatial_predicate.wasm', RawBLOB);

CREATE FUNCTION wasm_spatial2_always_true_multiarg
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_multi_arg_dispatch' :: 'always_true'
    ARGUMENTS (geom Point, poly1 Array(Tuple(Float64, Float64)), poly2 Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

SELECT id FROM file('$FILE', Parquet)
WHERE wasm_spatial2_always_true_multiarg(geometry, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)], [(50., 50.), (51., 50.), (51., 51.), (50., 51.), (50., 50.)])
ORDER BY id;
EOF
)
echo "${pruned:-no pruning}"

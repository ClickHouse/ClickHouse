#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
#
# Regression test: `extractSpatialFilters` (src/Processors/Formats/Impl/Parquet/GeoFilter.cpp), via the
# shared `collectSpatialFiltersConjunctive` template (src/Common/GeoBbox.h), silently dropped any `and`
# conjunct whose constant geometry argument failed to extract/validate instead of vetoing row-group
# pruning for the whole conjunction -- unlike `MergeTree`'s `spatial_bbox` index, whose
# `collectConjunctiveBbox` already fails the entire query bbox closed when any conjunct is
# `NodeBboxStatus::Failed` (see 04849_spatial_bbox_and_conjunct_scan_all.sql). A WASM UDF opted into
# pruning via `is_spatial_predicate = 1` (`always_true`, which never actually throws) combined via `AND`
# with a valid `pointInPolygon` conjunct: the invalid constant polygon on the UDF's side must still
# disable pruning driven by the *other*, individually-valid conjunct, because a spatial predicate with
# an invalid constant geometry argument is only "known safe to ignore" when nothing else in the
# conjunction depends on it going through cleanly -- the fail-closed contract applies to the whole `and`,
# not just the conjunct that failed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="$CUR_DIR/data_parquet/04059_geo_spatial_pruning.parquet"
WASM_CONFIG="$CUR_DIR/../../config/config.d/wasm_udf.xml"

# South Texas alone (ids 1,2) is disjoint from the north Texas row group (ids 3,4) and would legitimately
# prune it on its own -- but the second conjunct's constant argument is a self-intersecting (bowtie) ring,
# not `bg::is_valid`. That must veto pruning for the whole `and`, not just drop out of the filter list.
$CLICKHOUSE_LOCAL --config-file "$WASM_CONFIG" --queries-file /dev/stdin <<EOF
INSERT INTO system.webassembly_modules (name, code)
SELECT 'spatial_predicate_and_conjunct_veto', raw_blob FROM file('$CUR_DIR/wasm/spatial_predicate.wasm', RawBLOB);

CREATE FUNCTION wasm_spatial2_always_true_veto
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_and_conjunct_veto' :: 'always_true'
    ARGUMENTS (geom Point, other_id Int32, poly_const Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

SELECT '=== south texas rows (correct regardless of pruning) ===';
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
  AND wasm_spatial2_always_true_veto(geometry, id, [(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)])
ORDER BY id;
EOF

# No row group should be reported as pruned: the invalid constant argument on the second conjunct must
# disable pruning entirely, even though the first conjunct alone would prune the north Texas row group.
echo "=== pruned_row_groups (expect no pruning) ==="
pruned=$($CLICKHOUSE_LOCAL --config-file "$WASM_CONFIG" --print-profile-events --queries-file /dev/stdin <<EOF 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //'
INSERT INTO system.webassembly_modules (name, code)
SELECT 'spatial_predicate_and_conjunct_veto', raw_blob FROM file('$CUR_DIR/wasm/spatial_predicate.wasm', RawBLOB);

CREATE FUNCTION wasm_spatial2_always_true_veto
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'spatial_predicate_and_conjunct_veto' :: 'always_true'
    ARGUMENTS (geom Point, other_id Int32, poly_const Array(Tuple(Float64, Float64))) RETURNS UInt8
    SETTINGS serialization_format = 'CSV', is_spatial_predicate = 1;

SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
  AND wasm_spatial2_always_true_veto(geometry, id, [(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)])
ORDER BY id;
EOF
)
echo "${pruned:-no pruning}"

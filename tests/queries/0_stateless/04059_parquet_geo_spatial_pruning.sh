#!/usr/bin/env bash
# Tags: no-fasttest
#
# Test GeoParquet spatial row group pruning via covering.bbox column statistics.
# The test file has 2 row groups:
#   RG0: Points near south Texas (lat ~31): ids 1, 2
#   RG1: Points near north Texas (lat ~34): ids 3, 4
# Each row has bbox_xmin/ymin/xmax/ymax Float64 columns referenced by
# GeoParquet JSON covering.bbox metadata.
# A pointInPolygon filter covering only south Texas should prune RG1.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
CLICKHOUSE_BINARY=${CLICKHOUSE_BINARY:="$(dirname "$(dirname "$(dirname "$CURDIR")")")/build/programs/clickhouse"}
. "$CURDIR"/../shell_config.sh

FILE="$CURDIR/data_parquet/04059_geo_spatial_pruning.parquet"

# Baseline: all 4 rows without filter
echo "=== all rows ==="
$CLICKHOUSE_LOCAL -q "SELECT id FROM file('$FILE', Parquet) ORDER BY id"

# Filter to south Texas: should return ids 1, 2 and prune RG1
echo "=== south texas ==="
$CLICKHOUSE_LOCAL -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id"

# Filter to north Texas: should return ids 3, 4 and prune RG0
echo "=== north texas ==="
$CLICKHOUSE_LOCAL -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 33.), (-96., 33.), (-96., 36.), (-99., 36.), (-99., 33.)])
ORDER BY id"

# Filter outside any data: should return 0 rows
echo "=== no match (atlantic) ==="
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)])"

# Verify pruning via ProfileEvents: south Texas filter should prune 1 row group
echo "=== pruned_row_groups (pruning enabled) ==="
$CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id" 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //'

# Verify pruning is disabled by the setting
echo "=== pruned_row_groups (pruning disabled) ==="
disabled_out=$($CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id
SETTINGS input_format_parquet_spatial_filter_push_down=0" 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //')
echo "${disabled_out:-no pruning}"

# OR-safety: spatial predicate joined by OR must NOT prune any row group.
# The south Texas polygon is disjoint from RG1 (north Texas), but because it is
# under OR with a non-spatial predicate (id = 3 which lives in RG1), pruning RG1
# would be incorrect — id=3 must still be returned.
echo "=== or-safety (ids 1, 2, 3 expected, row group 1 not pruned) ==="
$CLICKHOUSE_LOCAL -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
   OR id = 3
ORDER BY id"

echo "=== or-safety no pruning expected ==="
or_pruned=$($CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
   OR id = 3
ORDER BY id" 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //')
echo "${or_pruned:-no pruning}"

# User-selected bbox column: bbox_xmin is both a covering.bbox column and a user output.
# Pruning must still prune RG1, AND bbox_xmin values must be decoded and returned correctly.
echo "=== user-selected bbox column with spatial filter ==="
$CLICKHOUSE_LOCAL -q "
SELECT id, bbox_xmin FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id"
echo "=== user-selected bbox column pruning count ==="
$CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT id, bbox_xmin FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id" 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //'

# bbox column used only in WHERE (not SELECT): covering.bbox column must be decoded for
# filter evaluation even though it is not a user output. Pruning must still prune RG1.
echo "=== bbox col in WHERE filter only ==="
$CLICKHOUSE_LOCAL -q "
SELECT id FROM file('$FILE', Parquet)
WHERE bbox_xmin > -98.5
  AND pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id"
echo "=== bbox col in WHERE filter only - pruning count ==="
$CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT id FROM file('$FILE', Parquet)
WHERE bbox_xmin > -98.5
  AND pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id" 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //'

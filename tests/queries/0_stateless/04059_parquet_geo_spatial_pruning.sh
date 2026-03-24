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

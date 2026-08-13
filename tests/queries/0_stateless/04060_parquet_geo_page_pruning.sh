#!/usr/bin/env bash
# Tags: no-fasttest
#
# Test GeoParquet spatial page-level pruning via covering.bbox column index.
# The test file has 1 row group with 3 pages (6 rows each):
#   Page 0: Points near south Texas (lat ~31): ids 1-6
#   Page 1: Points near north Texas (lat ~34): ids 7-12
#   Page 2: Points in Atlantic (lon ~-20): ids 13-18
# A south Texas filter should prune pages 1 and 2.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
CLICKHOUSE_BINARY=${CLICKHOUSE_BINARY:="$(dirname "$(dirname "$(dirname "$CURDIR")")")/build/programs/clickhouse"}
. "$CURDIR"/../shell_config.sh

FILE="$CURDIR/data_parquet/04060_geo_page_pruning.parquet"

echo "=== all rows ==="
$CLICKHOUSE_LOCAL -q "SELECT id FROM file('$FILE', Parquet) ORDER BY id"

echo "=== south texas ==="
$CLICKHOUSE_LOCAL -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 32.5), (-99., 32.5), (-99., 30.)])
ORDER BY id"

echo "=== north texas ==="
$CLICKHOUSE_LOCAL -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 33.), (-96., 33.), (-96., 35.5), (-99., 35.5), (-99., 33.)])
ORDER BY id"

echo "=== no match (atlantic) ==="
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)])"

echo "=== pruned_pages (south texas filter) ==="
$CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 32.5), (-99., 32.5), (-99., 30.)])
ORDER BY id" 2>&1 | grep 'ParquetPrunedPages' | sed 's/^.*] //'

echo "=== pruned_pages (pruning disabled) ==="
disabled_out=$($CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 32.5), (-99., 32.5), (-99., 30.)])
ORDER BY id
SETTINGS input_format_parquet_spatial_filter_push_down=0" 2>&1 | grep 'ParquetPrunedPages' | sed 's/^.*] //')
echo "${disabled_out:-no pruning}"

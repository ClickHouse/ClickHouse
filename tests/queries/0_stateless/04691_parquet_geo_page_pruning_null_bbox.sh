#!/usr/bin/env bash
# Tags: no-fasttest
#
# GeoParquet page-level spatial pruning must fail closed when the `covering.bbox` is incomplete.
#
# The test file has 1 row group of 18 rows and three column-index pages of 6 rows per bbox column:
#   ids 1-6:   south Texas points, complete covering
#   ids 7-12:  north Texas points, complete covering
#   ids 13-18: south Texas points whose `bbox_xmin` is NULL, while the three sibling bbox columns
#              describe a point in the Atlantic instead of the real geometry
#
# A NULL in any bbox column means the row's spatial extent is unknown, so no bbox column of that
# predicate may prune. Page boundaries are per column, so checking only the column being scanned
# is not enough: `bbox_ymax` has no NULLs at all and its third page would rule out ids 13-18 for a
# south Texas filter, dropping rows that the geometry itself matches.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILE="$CURDIR/data_parquet/04691_geo_page_pruning_null_bbox.parquet"

SOUTH_TEXAS="pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 32.5), (-99., 32.5), (-99., 30.)])"

echo "=== all rows ==="
$CLICKHOUSE_LOCAL -q "SELECT id FROM file('$FILE', Parquet) ORDER BY id"

echo "=== south texas (spatial pruning on) ==="
$CLICKHOUSE_LOCAL -q "SELECT id FROM file('$FILE', Parquet) WHERE $SOUTH_TEXAS ORDER BY id"

echo "=== south texas (spatial pruning off) ==="
$CLICKHOUSE_LOCAL -q "
SELECT id FROM file('$FILE', Parquet) WHERE $SOUTH_TEXAS ORDER BY id
SETTINGS input_format_parquet_spatial_filter_push_down = 0"

echo "=== south texas (page pruning off) ==="
$CLICKHOUSE_LOCAL -q "
SELECT id FROM file('$FILE', Parquet) WHERE $SOUTH_TEXAS ORDER BY id
SETTINGS input_format_parquet_page_filter_push_down = 0"

echo "=== pruned_pages (south texas) ==="
pruned=$($CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT id FROM file('$FILE', Parquet) WHERE $SOUTH_TEXAS ORDER BY id" 2>&1 | grep 'ParquetPrunedPages' | sed 's/^.*] //')
echo "${pruned:-no pruning}"

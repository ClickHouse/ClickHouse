#!/usr/bin/env bash
# Tags: no-fasttest
#
# `covering.bbox` columns exist purely to help row-group pruning (they're never part of the
# query's own output or WHERE columns). `getHyperrectangleForRowGroup` used to unconditionally
# rethrow on a column-chunk statistics decode failure, citing
# `input_format_parquet_filter_push_down=0` as an escape hatch - which wouldn't even disable this
# path, and aborted the whole read over stats that are optimization-only. Malformed helper stats
# must fail closed (skip pruning for that row group) instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Same fixture as 04512 (2 row groups: south Texas ids 1,2; north Texas ids 3,4; real
# `covering.bbox` pointing at bbox_xmin/ymin/xmax/ymax), except row group 0's `bbox_xmin`
# column-chunk statistics min_value has been truncated to 2 bytes (too short to decode as a
# Float64) - simulating a corrupt/foreign-writer Parquet file.
FILE="$CUR_DIR/data_parquet/04515_geo_pruning_iceberg_malformed_bbox.parquet"

# South Texas filter is disjoint from the (well-formed) north Texas row group, but row group 0's
# bbox_xmin stats are malformed. With the fix: the read still succeeds and returns both south
# Texas rows (row group 0 not pruned - fails closed - but not aborted either); without the fix,
# this query throws instead of returning rows.
echo "=== query succeeds despite malformed covering.bbox stats ==="
$CLICKHOUSE_LOCAL -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id"

#!/usr/bin/env bash
# Tags: no-fasttest
#
# A `covering.bbox` column can also be filtered on by the query itself. Malformed column-chunk
# statistics for such a column must keep the generic min/max contract: throw (surfacing the
# corruption), with `input_format_parquet_filter_push_down=0` as the escape hatch. Only stats
# reads done purely for spatial pruning fail closed (see 04515). Without the fix, the spatial
# path swallowed the decode error for every `covering.bbox` primitive, silently downgrading the
# query's own `bbox_xmin` min/max filter.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Same fixture as 04515: 2 row groups (south Texas ids 1,2; north Texas ids 3,4), real
# `covering.bbox` pointing at bbox_xmin/ymin/xmax/ymax, and row group 0's `bbox_xmin`
# column-chunk statistics min_value truncated to 2 bytes (fails to decode as Float64).
FILE="$CUR_DIR/data_parquet/04515_geo_pruning_iceberg_malformed_bbox.parquet"

# The query's own `KeyCondition` references `bbox_xmin`, so the generic min/max path would have
# decoded its stats even without the spatial predicate: the malformed stats must surface.
echo "=== direct bbox predicate + spatial predicate: malformed stats throw ==="
$CLICKHOUSE_LOCAL -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
    AND bbox_xmin > -200.
ORDER BY id" 2>&1 | grep -o -m1 "use input_format_parquet_filter_push_down=0 to ignore"

# The documented escape hatch works: with `input_format_parquet_filter_push_down=0` the generic
# min/max path is off, spatial pruning fails closed on the malformed row group, and the read
# succeeds with the WHERE still applied to the data.
echo "=== escape hatch: filter_push_down=0 reads through ==="
$CLICKHOUSE_LOCAL -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
    AND bbox_xmin > -200.
ORDER BY id
SETTINGS input_format_parquet_filter_push_down = 0"

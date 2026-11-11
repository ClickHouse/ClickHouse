#!/usr/bin/env bash
# Tags: no-fasttest
#
# GeoParquet spatial pruning must not hide the "Polygon is not valid" exception.
# `tryExtractBboxFromColumn` (src/Common/GeoBbox.h) builds a pruning bbox directly
# from a constant polygon literal; if it didn't validate the ring the same way
# `parseConstPolygon` does at execute time, a self-intersecting polygon whose bbox
# is disjoint from every row group could prune all of them and return 0 rows
# silently instead of throwing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="$CUR_DIR/data_parquet/04059_geo_spatial_pruning.parquet"

# Self-intersecting (bowtie) polygon, bbox disjoint from all row groups. With
# validate_polygons = 1 (the default) this must throw, not silently return 0 rows.
echo "=== invalid polygon literal still throws (validate_polygons=1, default) ==="
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)])
" 2>&1 | grep -o "Polygon is not valid"

# Sanity check: a valid polygon literal must still engage row-group pruning by
# default (regression guard for the fix above not disabling pruning wholesale).
echo "=== valid polygon literal still prunes by default ==="
$CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id" 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //'

# A polygon-with-holes literal where the hole lies entirely outside the shell is invalid
# (each ring is individually simple, but the assembled polygon is not) — must still throw,
# not be pruned away based on the union bbox of the two rings validated in isolation.
echo "=== hole outside shell still throws (not validated ring-by-ring) ==="
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [[(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)], [(3., 3.), (4., 3.), (4., 4.), (3., 4.), (3., 3.)]])
" 2>&1 | grep -o "Polygon is not valid"

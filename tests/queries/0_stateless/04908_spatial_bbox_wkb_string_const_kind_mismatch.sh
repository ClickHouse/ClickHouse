#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: a WKB-encoded `String` constant used to be trusted as `spatial_bbox` pruning
# input by every spatial predicate, even though none of the builtins accepts a `String` geometry
# argument at any position -- `pointInPolygon` raises `ILLEGAL_TYPE_OF_ARGUMENT` and
# `polygonsIntersectCartesian`/`polygonsWithinCartesian` raise `BAD_ARGUMENTS` (`Unknown geometry
# type String`) from `callOnGeometryDataType`'s dispatch. When the `String` arrives inside a
# `Dynamic`/`Variant` constant, the raising overload is only built once the predicate is actually
# evaluated on a row, so a bbox derived from the WKB payload let the index prune every granule and
# answer `0` instead of surfacing the exception. `constGeoKindName` (`src/Common/GeoBbox.h`) now
# reports a `String` under the kind name `String`, which all three builtins reject.
#
# `clickhouse-local` rather than the server: with every granule pruned away the server still
# evaluates the predicate on the empty chunk left behind, which surfaces the exception by accident;
# in `clickhouse-local` nothing is evaluated at all and the fail-open answer `0` is user-visible.

SCHEMA="
CREATE TABLE t (id UInt32, poly Polygon, INDEX idx_bbox poly TYPE spatial_bbox GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;
-- Every granule's bbox is far from the geometries queried below, so a bbox derived from a WKB
-- \`String\` that should have been rejected prunes everything and hides the exception.
INSERT INTO t SELECT number + 1, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]] FROM numbers(4);
"

FAR_POINT="wkb(CAST((500., 500.) AS Point))"
FAR_POLYGON="wkb(CAST([[(500., 500.), (501., 500.), (501., 501.), (500., 500.)]] AS Polygon))"

echo "=== pointInPolygon: a Dynamic-wrapped WKB String must raise, not prune every granule ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE pointInPolygon($FAR_POINT::Dynamic, poly);" 2>&1 | grep -o "must contain a tuple"

echo "=== polygonsIntersectCartesian: same, via a Dynamic-wrapped WKB String ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE polygonsIntersectCartesian(poly, $FAR_POLYGON::Dynamic);" 2>&1 | grep -o "Unknown geometry type String"

echo "=== polygonsWithinCartesian: same, via a Dynamic-wrapped WKB String ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE polygonsWithinCartesian(poly, $FAR_POLYGON::Dynamic);" 2>&1 | grep -o "Unknown geometry type String"

echo "=== plain WKB String constant, with no Dynamic wrapper, must raise too ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE polygonsIntersectCartesian(poly, $FAR_POLYGON);" 2>&1 | grep -o "Unknown geometry type String"

# Sanity: this must only disable pruning for arguments the predicate is guaranteed to reject -- a
# legitimate constant `Point` must still prune every granule and answer `0` without raising.
echo "=== sanity: a legitimate constant Point still prunes ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE pointInPolygon(CAST((500., 500.) AS Point), poly);
SELECT count() FROM t WHERE pointInPolygon(CAST((0.5, 0.5) AS Point), poly);"

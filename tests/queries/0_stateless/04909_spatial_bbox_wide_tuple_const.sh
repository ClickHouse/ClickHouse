#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: `spatial_bbox` pruning used to accept a constant tuple with *at least* two
# elements as a point, and a ring whose vertices have more than two coordinates, while the
# predicates require exactly two: `pointInPolygon` raises `BAD_ARGUMENTS` ("must have exactly two
# elements") and `callOnGeometryDataType` raises `BAD_ARGUMENTS` ("Unknown geometry type") for an
# `Array(Tuple(Float64, Float64, Float64))`. A wide tuple arriving inside a `Dynamic`/`Variant`
# constant passes argument type-checking during analysis, so the bbox derived from its first two
# coordinates let the index prune every granule and answer `0` instead of surfacing the exception.
#
# `clickhouse-local` rather than the server: with every granule pruned away the server still
# evaluates the predicate on the empty chunk left behind, which surfaces the exception by accident;
# in `clickhouse-local` nothing is evaluated at all and the fail-open answer `0` is user-visible.

SCHEMA="
CREATE TABLE t (id UInt32, poly Polygon, INDEX idx_bbox poly TYPE spatial_bbox GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;
-- Every granule's bbox is far from the geometries queried below, so a bbox derived from a wide
-- tuple that should have been rejected prunes everything and hides the exception.
INSERT INTO t SELECT number + 1, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]] FROM numbers(4);
"

WIDE_POINT="CAST((500., 500., 1.), 'Tuple(Float64, Float64, Float64)')"
WIDE_RING="CAST([(500., 500., 1.), (501., 500., 1.), (501., 501., 1.), (500., 500., 1.)], 'Array(Tuple(Float64, Float64, Float64))')"

echo "=== pointInPolygon: a Dynamic-wrapped three-element point tuple must raise, not prune ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE pointInPolygon($WIDE_POINT::Dynamic, poly);" 2>&1 | grep -o "must have exactly two elements"

echo "=== polygonsIntersectCartesian: a Dynamic-wrapped ring of wide vertices must raise too ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE polygonsIntersectCartesian(poly, $WIDE_RING::Dynamic);" 2>&1 | grep -o "Unknown geometry type Array(Tuple(Float64, Float64, Float64))"

echo "=== polygonsWithinCartesian: same ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE polygonsWithinCartesian(poly, $WIDE_RING::Dynamic);" 2>&1 | grep -o "Unknown geometry type Array(Tuple(Float64, Float64, Float64))"

# Sanity: only arguments the predicate is guaranteed to reject lose pruning -- a well-formed
# two-element point and a well-formed ring must still prune every granule and answer `0`.
echo "=== sanity: well-formed constants still prune ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE pointInPolygon(CAST((500., 500.) AS Point), poly);
SELECT count() FROM t WHERE pointInPolygon(CAST((0.5, 0.5) AS Point), poly);
SELECT count() FROM t WHERE polygonsIntersectCartesian(poly, CAST([[(500., 500.), (501., 500.), (501., 501.), (500., 500.)]] AS Polygon));
SELECT count() FROM t WHERE polygonsIntersectCartesian(poly, CAST([[(0.3, 0.3), (0.7, 0.3), (0.7, 0.7), (0.3, 0.3)]] AS Polygon));"

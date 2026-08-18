#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: `geoKindNameOfType` (src/Common/GeoBbox.h) inspected only the OUTER type, so a
# geometry kind hidden under a `Nullable` wrapper was reported as no kind at all. A WKB-encoded
# `String` is rejected by every builtin spatial predicate and is reported under the kind name
# `String` (see 04908_spatial_bbox_wkb_string_const_kind_mismatch), but wrapping it as
# `Nullable(String)` hid that: `tryExtractConstGeoField` flattens the non-null value to a plain
# `String` `Field`, `extractBboxFromFieldValue` decoded a perfectly usable bbox from the payload,
# and the index pruned every granule away.
#
# At execution time `useDefaultImplementationForNulls` strips the `Nullable` and the nested `String`
# is rejected as usual -- but on a fully pruned granule that never runs: for nullable inputs on a
# `0`-row block `IFunction` returns an empty result without calling the nested function at all.
# The answer is then a silent `0` instead of the `BAD_ARGUMENTS`/`ILLEGAL_TYPE_OF_ARGUMENT` the
# query must surface.
#
# `clickhouse-local` rather than the server, for the same reason as 04908: with every granule pruned
# the server still evaluates the predicate on the empty chunk left behind, which surfaces the
# exception by accident; in `clickhouse-local` nothing is evaluated and the fail-open answer `0` is
# user-visible.

SCHEMA="
CREATE TABLE t (id UInt32, poly Polygon, INDEX idx_bbox poly TYPE spatial_bbox GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;
-- Every granule's bbox is far from the geometries queried below, so a bbox derived from a wrapped
-- kind that should have been rejected prunes everything and hides the exception.
INSERT INTO t SELECT number + 1, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]] FROM numbers(4);
"

FAR_POINT="wkb(CAST((500., 500.) AS Point))"
FAR_POLYGON="wkb(CAST([[(500., 500.), (501., 500.), (501., 501.), (500., 500.)]] AS Polygon))"

echo "=== pointInPolygon: a Nullable-wrapped WKB String must raise, not prune every granule ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE pointInPolygon(CAST($FAR_POINT AS Nullable(String)), poly);" 2>&1 | grep -o "must contain a tuple"

echo "=== polygonsIntersectCartesian: same, via a Nullable-wrapped WKB String ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE polygonsIntersectCartesian(poly, CAST($FAR_POLYGON AS Nullable(String)));" 2>&1 | grep -o "Unknown geometry type String"

echo "=== polygonsWithinCartesian: same, via a Nullable-wrapped WKB String ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE polygonsWithinCartesian(poly, CAST($FAR_POLYGON AS Nullable(String)));" 2>&1 | grep -o "Unknown geometry type String"

echo "=== a Nullable-wrapped geometry constant of an ACCEPTED kind must still prune ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE pointInPolygon(CAST((0.5, 0.5) AS Point), poly);"

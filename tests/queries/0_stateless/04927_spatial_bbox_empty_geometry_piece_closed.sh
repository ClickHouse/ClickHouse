#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: `extractBboxFromFieldValue` (src/Common/GeoBbox.h) failed OPEN for an EMPTY
# geometry piece inside a single-column constant `Polygon`/`MultiPolygon`. `isRingArray` requires a
# non-empty array, so `isPolygonArray` rejected `[shell, []]` and the value missed the assembled-
# geometry branch, landing in the generic array recursion instead. That recursion kept the bbox of
# the non-empty shell and silently skipped the empty hole, yielding a perfectly usable bbox.
#
# `pointInPolygon` does not treat that value as harmless: `parseConstPolygon` assembles the same
# literal as one polygon-with-holes and validates it with `bg::is_valid`, so the query must raise
# `BAD_ARGUMENTS` -- but with every disjoint granule pruned it never gets to. For the same reason a
# top-level `CAST([], 'Ring')`/`CAST([], 'Polygon')` was reported as `NoInfo` rather than `Failed`,
# so a sibling conjunct could hide the exception too.
#
# `clickhouse-local` rather than the server, for the same reason as
# 04908_spatial_bbox_wkb_string_const_kind_mismatch: with every granule pruned the server still
# evaluates the predicate on the empty chunk left behind, which surfaces the exception by accident;
# in `clickhouse-local` nothing is evaluated and the fail-open answer `0` is user-visible.

SCHEMA="
CREATE TABLE t (id UInt32, p Point, INDEX idx_bbox p TYPE spatial_bbox GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;
INSERT INTO t SELECT number + 1, (0.5, 0.5) FROM numbers(4);
"

echo "=== a constant polygon with an empty hole must raise, not prune every granule ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE pointInPolygon(p, [[(100., 100.), (101., 100.), (101., 101.), (100., 100.)], []]);" 2>&1 | grep -o "Polygon is not valid"

echo "=== a constant multipolygon with an empty polygon must raise too ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE pointInPolygon(p, CAST([[[(100., 100.), (101., 100.), (101., 101.), (100., 100.)]], []] AS MultiPolygon));" 2>&1 | grep -o "not valid\|Cannot convert\|BAD_ARGUMENTS" | head -1

echo "=== an empty Ring constant must fail closed, so a sibling conjunct cannot hide it ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE pointInPolygon(p, CAST([] AS Ring)) OR id > 1000000;" 2>&1 | grep -o "Polygon is not valid"

echo "=== a well-formed polygon with a real hole must still prune ==="
$CLICKHOUSE_LOCAL -q "$SCHEMA
SELECT count() FROM t WHERE pointInPolygon(p, [[(100., 100.), (110., 100.), (110., 110.), (100., 110.), (100., 100.)], [(102., 102.), (104., 102.), (104., 104.), (102., 104.), (102., 102.)]]);"

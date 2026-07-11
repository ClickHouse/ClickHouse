#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Write a large real-world FeatureCollection (US counties, ~3200 features with large polygons) with
# parallel formatting and check the result is a single valid GeoJSON document that preserves every
# feature. A small max_block_size forces many chunks so the parallel formatter is actually exercised.
# Row order is not asserted (SELECT * is unordered, so parallel output need not match sequential
# byte-for-byte); instead an order-independent hash over every column confirms the same set of features,
# with the same id, geometry, and properties, round-trips through the output.
F="$CUR_DIR/data_geojson/us_counties.geojson.zst"
OUT="$CLICKHOUSE_TMP/geojson_counties_parallel.geojson"

${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('$F', GeoJSON) FORMAT GeoJSON SETTINGS output_format_parallel_formatting = 1, max_block_size = 16" > "$OUT"

# The parallel output is a single valid JSON document.
${CLICKHOUSE_LOCAL} -q "SELECT isValidJSON(c) FROM file('$OUT', RawBLOB, 'c String')"
# Every feature's content round-trips: an order-independent content hash of the output matches the source.
SRC=$(${CLICKHOUSE_LOCAL} -q "SELECT sum(sipHash64(id, toString(geometry), toString(properties))) FROM file('$F', GeoJSON)")
GOT=$(${CLICKHOUSE_LOCAL} -q "SELECT sum(sipHash64(id, toString(geometry), toString(properties))) FROM file('$OUT', GeoJSON)")
[ "$SRC" = "$GOT" ] && echo 1 || echo 0

rm -f "$OUT"

#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Each GeoJSON output is one complete top-level FeatureCollection, so the format does not support
# appending to an existing file. A second insert without truncation is rejected rather than producing
# a malformed document containing two collections.
OUT="$CLICKHOUSE_TMP/geojson_no_append.geojson"
rm -f "$OUT"

${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('$OUT', GeoJSON) SELECT (1.0, 2.0)::Point AS geometry SETTINGS engine_file_truncate_on_insert = 1"
${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('$OUT', GeoJSON) SELECT (3.0, 4.0)::Point AS geometry" >/dev/null 2>&1 \
    && echo "append accepted" || echo "append rejected"

rm -f "$OUT"

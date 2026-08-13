#!/usr/bin/env bash
#
# GeoJSON input format: recursion is bounded by `input_format_json_max_depth` so a pathologically
# nested document raises TOO_DEEP_RECURSION instead of exhausting the parser stack. This applies to
# recursively validated GeometryCollections and to strictly skipped ignored values (foreign members),
# both of which recurse in the parser. The limit is lowered here so the check is exercised cheaply and
# deterministically rather than by generating a stack-overflowing document.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Wrap an innermost geometry in N nested GeometryCollections.
gen_nested_gc() {
    local n=$1 inner='{"type":"Point","coordinates":[1,2]}'
    local i
    for ((i = 0; i < n; i++)); do
        inner='{"type":"GeometryCollection","geometries":['"$inner"']}'
    done
    printf '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":%s,"properties":{}}]}' "$inner"
}

# A FeatureCollection with an ignored foreign member holding N nested JSON arrays (or objects).
gen_nested_ignored() {
    local n=$1 open=$2 close=$3 inner='1'
    local i
    for ((i = 0; i < n; i++)); do
        inner="${open}${inner}${close}"
    done
    printf '{"type":"FeatureCollection","foreign":%s,"features":[{"type":"Feature","geometry":null,"properties":{}}]}' "$inner"
}

# A modestly nested but valid GeometryCollection parses within the default depth limit.
${CLICKHOUSE_LOCAL} --query "
    SELECT 'nested GC within limit', variantType(geometry)
    FROM format(GeoJSON, '$(gen_nested_gc 3)')
    SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'"

# The same shape, nested past a lowered limit, raises TOO_DEEP_RECURSION rather than overflowing.
${CLICKHOUSE_LOCAL} --query "
    SELECT count() FROM format(GeoJSON, '$(gen_nested_gc 50)')
    SETTINGS input_format_geojson_unsupported_geometry_handling = 'null', input_format_json_max_depth = 10" 2>&1 \
    | grep -om1 TOO_DEEP_RECURSION

# A deeply nested ignored value (a foreign member) is bounded the same way by the strict skipper,
# for both nested arrays and nested objects.
${CLICKHOUSE_LOCAL} --query "
    SELECT count() FROM format(GeoJSON, '$(gen_nested_ignored 50 '[' ']')')
    SETTINGS input_format_json_max_depth = 10" 2>&1 \
    | grep -om1 TOO_DEEP_RECURSION

${CLICKHOUSE_LOCAL} --query "
    SELECT count() FROM format(GeoJSON, '$(gen_nested_ignored 50 '{"a":' '}')')
    SETTINGS input_format_json_max_depth = 10" 2>&1 \
    | grep -om1 TOO_DEEP_RECURSION

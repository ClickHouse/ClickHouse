#!/usr/bin/env bash
# Tags: no-fasttest
# Source: https://datahub.io/core/geo-countries/r/0.geojson

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE countries (
        geometry Geometry,
        properties JSON
    ) ENGINE = MergeTree ORDER BY tuple()"

cat "$CUR_DIR/data_geojson/countries.geojson" \
    | ${CLICKHOUSE_CLIENT} -q "INSERT INTO countries FORMAT GeoJSON"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM countries"

${CLICKHOUSE_CLIENT} -q "
    SELECT variantType(geometry) AS geo_type, count() AS cnt
    FROM countries
    GROUP BY geo_type
    ORDER BY geo_type"

${CLICKHOUSE_CLIENT} -q "
    SELECT JSONExtractString(properties, 'name')
    FROM countries
    WHERE JSONExtractString(properties, 'ISO3166-1-Alpha-2') IN ('DE', 'GB')
    ORDER BY 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE countries"

#!/usr/bin/env bash
# Source: https://datahub.io/core/geo-countries/r/0.geojson

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE countries (
        geometry Geometry,
        properties JSON
    ) ENGINE = MergeTree ORDER BY tuple()"

${CLICKHOUSE_CLIENT} -q "
    INSERT INTO countries
    FROM INFILE '$CUR_DIR/data_geojson/countries.geojson.zst'
    FORMAT GeoJSON"

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

# An empty `features` array produces zero rows. This is tested via a standalone INSERT rather than
# `format()` with zero rows (a known ClickHouse limitation) or an inline INSERT inside a multi-query
# script: in the latter the following statements remain in the format's read buffer and are rejected
# by the strict trailing-data check, the same way `JSONEachRow` rejects them.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE empty_features (id String, geometry Geometry, properties JSON) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q 'INSERT INTO empty_features FORMAT GeoJSON {"type":"FeatureCollection","features":[]}'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM empty_features"
${CLICKHOUSE_CLIENT} -q "DROP TABLE empty_features"

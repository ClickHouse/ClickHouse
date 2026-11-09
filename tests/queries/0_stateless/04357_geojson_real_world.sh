#!/usr/bin/env bash
# Tags: long

# Reads real-world public-domain GeoJSON datasets and exercises the parsed geometries with the geo
# functions, so the test proves the geometry values are correct and usable, not merely parseable.
#   Cities (Point):  https://github.com/nvkelso/natural-earth-vector (Natural Earth, public domain)
#   Rivers (LineString):  https://github.com/nvkelso/natural-earth-vector (Natural Earth, public domain)
#   US counties (Polygon/MultiPolygon, FIPS feature ids):  U.S. Census cartographic boundary
#     (public domain), via https://github.com/plotly/datasets/blob/master/geojson-counties-fips.json

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Cities are a FeatureCollection of Point geometries with rich properties, preceded by top-level
# `name` and `crs` members that the parser skips. The table stays alive until the end because the
# final cross-dataset query joins these points against the county polygons.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE cities (geometry Geometry, properties JSON) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "INSERT INTO cities FROM INFILE '$CUR_DIR/data_geojson/ne_110m_populated_places.geojson.zst' FORMAT GeoJSON"
${CLICKHOUSE_CLIENT} -q "SELECT 'How many cities?', count() FROM cities"
${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT 'City geometry type?', variantType(geometry) FROM cities"
${CLICKHOUSE_CLIENT} -q "
    WITH
        (SELECT geometry.Point FROM cities WHERE JSONExtractString(properties, 'NAME') = 'London') AS lp,
        (SELECT geometry.Point FROM cities WHERE JSONExtractString(properties, 'NAME') = 'Paris') AS pp,
        (SELECT geometry.Point FROM cities WHERE JSONExtractString(properties, 'NAME') = 'New York') AS np
    SELECT 'London-Paris and London-New York distance (km)?', round(geoDistance(lp.1, lp.2, pp.1, pp.2) / 1000), round(geoDistance(lp.1, lp.2, np.1, np.2) / 1000)"
${CLICKHOUSE_CLIENT} -q "SELECT 'Northernmost city?', JSONExtractString(properties, 'NAME') AS name FROM cities ORDER BY geometry.Point.2 DESC, name LIMIT 1"
${CLICKHOUSE_CLIENT} -q "SELECT 'Southernmost city?', JSONExtractString(properties, 'NAME') AS name FROM cities ORDER BY geometry.Point.2 ASC, name LIMIT 1"
${CLICKHOUSE_CLIENT} -q "SELECT 'Most populous city and its population?', JSONExtractString(properties, 'NAME') AS name, JSONExtractUInt(properties, 'POP_MAX') AS pop FROM cities ORDER BY pop DESC, name LIMIT 1"

# Rivers are LineString geometries; the length of each is the sum of the great-circle distances
# between consecutive points, and the longest one is reported with its rounded length and point count.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE rivers (geometry Geometry, properties JSON) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "INSERT INTO rivers FROM INFILE '$CUR_DIR/data_geojson/ne_110m_rivers_lake_centerlines.geojson.zst' FORMAT GeoJSON"
${CLICKHOUSE_CLIENT} -q "SELECT 'How many rivers?', count() FROM rivers"
${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT 'River geometry type?', variantType(geometry) FROM rivers"
${CLICKHOUSE_CLIENT} -q "
    SELECT
        'Longest river, its length (km) and point count?',
        JSONExtractString(properties, 'name') AS name,
        round(arraySum(arrayMap((a, b) -> geoDistance(a.1, a.2, b.1, b.2), arrayPopBack(geometry.LineString), arrayPopFront(geometry.LineString))) / 1000) AS length_km,
        length(geometry.LineString) AS points
    FROM rivers
    ORDER BY length_km DESC, name
    LIMIT 1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE rivers"

# US counties are Polygon and MultiPolygon geometries; each Feature carries its FIPS code as the id.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE counties (id String, geometry Geometry, properties JSON) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "INSERT INTO counties FROM INFILE '$CUR_DIR/data_geojson/us_counties.geojson.zst' FORMAT GeoJSON"
${CLICKHOUSE_CLIENT} -q "SELECT 'How many counties?', count() FROM counties"
${CLICKHOUSE_CLIENT} -q "SELECT 'County geometry types and counts?', variantType(geometry) AS geo_type, count() AS cnt FROM counties GROUP BY geo_type ORDER BY geo_type"
${CLICKHOUSE_CLIENT} -q "SELECT 'States with the most counties (state FIPS, count)?', substring(id, 1, 2) AS state_fips, count() AS cnt FROM counties GROUP BY state_fips ORDER BY cnt DESC, state_fips LIMIT 3"
${CLICKHOUSE_CLIENT} -q "SELECT 'Largest county by area (FIPS, name)?', id, JSONExtractString(properties, 'NAME') FROM counties ORDER BY areaSpherical(geometry) DESC, id LIMIT 1"
${CLICKHOUSE_CLIENT} -q "SELECT 'Does a county geometry serialise to a WKT POLYGON?', startsWith(wkt(geometry), 'POLYGON') FROM counties WHERE id = '01001'"

# Cross-dataset spatial join: the county that contains New York's point. A Polygon is wrapped as a
# single-element MultiPolygon so polygons and multipolygons are queried uniformly.
${CLICKHOUSE_CLIENT} -q "
    SELECT 'Which county contains New York (FIPS, name, state FIPS)?', id, JSONExtractString(properties, 'NAME'), substring(id, 1, 2)
    FROM counties
    WHERE pointInPolygon(
        (SELECT geometry.Point FROM cities WHERE JSONExtractString(properties, 'NAME') = 'New York'),
        if(variantType(geometry) = 'MultiPolygon', geometry.MultiPolygon, [geometry.Polygon]))
    ORDER BY id"

${CLICKHOUSE_CLIENT} -q "DROP TABLE counties"
${CLICKHOUSE_CLIENT} -q "DROP TABLE cities"

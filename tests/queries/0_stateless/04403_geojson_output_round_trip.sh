#!/usr/bin/env bash
# Tags: long
#
# Round-trips real-world GeoJSON through the output format: read a dataset, write it back with
# FORMAT GeoJSON, re-read the emitted document, and assert the two parsed tables are identical. This
# proves coordinates (Float64 -> shortest text -> Float64 is exact), geometry types, properties
# (canonical JSON is stable across re-parse), and ids all survive a read -> write -> read cycle.
#   Cities (Point):  Natural Earth populated places (public domain)
#   Rivers (LineString):  Natural Earth rivers/lake centerlines (public domain)
#   US counties (Polygon/MultiPolygon, FIPS feature ids):  U.S. Census cartographic boundary (public domain)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCHEMA="id Nullable(String), geometry Geometry, properties Nullable(JSON)"

roundtrip() {
    local name=$1 file=$2
    local src="$CUR_DIR/data_geojson/$file"
    local out="$CLICKHOUSE_TMP/rt_${name}.geojson"

    ${CLICKHOUSE_LOCAL} --query "
        CREATE TABLE orig ($SCHEMA) ENGINE = Memory;
        INSERT INTO orig FROM INFILE '$src' FORMAT GeoJSON;

        -- Read the original, write it back as GeoJSON.
        SELECT * FROM orig INTO OUTFILE '$out' TRUNCATE FORMAT GeoJSON;

        -- Re-read the emitted document.
        CREATE TABLE rt ($SCHEMA) ENGINE = Memory;
        INSERT INTO rt FROM INFILE '$out' FORMAT GeoJSON;

        -- The emitted document is a single FeatureCollection.
        SELECT '$name wrapper', startsWith(line, '{\"type\":\"FeatureCollection\",\"features\":[')
        FROM file('$out', LineAsString, 'line String') LIMIT 1;

        -- Row counts match, and the re-parsed table is identical to the original parse (an
        -- order-independent comparison of id, geometry text, and canonical properties JSON).
        SELECT '$name counts', (SELECT count() FROM orig) = (SELECT count() FROM rt);
        SELECT '$name identical',
            (SELECT count() FROM (SELECT id, toString(geometry) g, toString(properties) p FROM orig
                                  EXCEPT
                                  SELECT id, toString(geometry) g, toString(properties) p FROM rt)) = 0
            AND (SELECT count() FROM (SELECT id, toString(geometry) g, toString(properties) p FROM rt
                                      EXCEPT
                                      SELECT id, toString(geometry) g, toString(properties) p FROM orig)) = 0;
    "
    rm -f "$out"
}

roundtrip "cities"   "ne_110m_populated_places.geojson.zst"
roundtrip "rivers"   "ne_110m_rivers_lake_centerlines.geojson.zst"
roundtrip "counties" "us_counties.geojson.zst"

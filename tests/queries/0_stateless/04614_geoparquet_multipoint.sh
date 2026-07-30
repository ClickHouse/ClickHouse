#!/usr/bin/env bash
# Tags: no-fasttest
#       ^ no Parquet support in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for typed `MultiPoint` support in GeoParquet:
# the writer must emit `MultiPoint` in the `geo` metadata and the reader must map it back
# to the `MultiPoint` type instead of a plain `String` with WKB bytes.

# Typed MultiPoint column: write to Parquet with geo metadata, read back typed.
$CLICKHOUSE_LOCAL --query="
    CREATE TABLE geo_mp (id Int32, mp MultiPoint) ENGINE = Memory();
    INSERT INTO geo_mp VALUES (1, [(10, 20), (30, 40)]);
    INSERT INTO geo_mp VALUES (2, [(-1.5, 2.25)]);
    SELECT * FROM geo_mp ORDER BY id FORMAT Parquet;" > "${CLICKHOUSE_TMP}/parquet_geo_mp.parquet"
$CLICKHOUSE_LOCAL --query="SELECT toTypeName(mp), * FROM file('${CLICKHOUSE_TMP}/parquet_geo_mp.parquet', Parquet) ORDER BY id;"

# Mixed Geometry column containing a MultiPoint: geometry_types has multiple entries,
# so reading must produce the Geometry type and preserve each alternative.
# The data file declares geometry_types = ['Point', 'MultiPoint', 'LineString'] and stores WKB values.
$CLICKHOUSE_LOCAL --query="
    SELECT toTypeName(g), variantType(g), g FROM file('$CUR_DIR/data_parquet/04614_geoparquet_multipoint_mixed.parquet', Parquet) ORDER BY id;"

rm -f "${CLICKHOUSE_TMP}/parquet_geo_mp.parquet"

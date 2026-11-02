#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet geo types are not tested in the fast test, like in the other GeoParquet tests.

# Files with the Parquet `GEOMETRY` and `GEOGRAPHY` logical types from apache/parquet-testing
# (`data/geospatial/`): `crs-default.parquet`, `crs-geography.parquet`, `crs-srid.parquet` (written
# by Arrow C++) and `geography-points.parquet` (written by DataFusion, 50 row groups with
# `GeospatialStatistics`, some of them with a bounding box crossing the antimeridian, `xmin > xmax`).
# Also spatial row group pruning by `GeospatialStatistics` of files written by ClickHouse.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_parquet"

for name in crs_default crs_geography crs_srid geography_points
do
    echo "--- $name"
    $CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA/05361_geospatial_$name.parquet', Parquet)"
done

echo "--- crs_default, crs_geography: the same polygon, as GEOMETRY and as GEOGRAPHY"
$CLICKHOUSE_LOCAL -q "
    SELECT length(geometry[1]), geometry[1][1], round(areaCartesian(geometry), 6), startsWith(wkt, 'POLYGON ((-111 45')
    FROM file('$DATA/05361_geospatial_crs_default.parquet', Parquet)"
$CLICKHOUSE_LOCAL -q "
    SELECT length(geography[1]), geography[1][1], round(areaCartesian(geography), 6)
    FROM file('$DATA/05361_geospatial_crs_geography.parquet', Parquet)"
echo "--- crs_srid: the CRS is not interpreted"
$CLICKHOUSE_LOCAL -q "SELECT length(geometry[1]), round(geometry[1][1].1, 3) FROM file('$DATA/05361_geospatial_crs_srid.parquet', Parquet)"

echo "--- geography_points"
POINTS="$DATA/05361_geospatial_geography_points.parquet"
$CLICKHOUSE_LOCAL -q "SELECT count(), round(min(geometry.1), 3), round(max(geometry.1), 3), round(min(geometry.2), 3), round(max(geometry.2), 3) FROM file('$POINTS', Parquet)"

# Near the antimeridian: only row groups 28 and 29 can match, the bounding box of 29 wraps around.
FILTER="pointInPolygon(geometry, [(170., -20.), (180., -20.), (180., 0.), (170., 0.), (170., -20.)])"
echo "--- pruning near the antimeridian"
$CLICKHOUSE_LOCAL -q "SELECT id, geometry FROM file('$POINTS', Parquet) WHERE $FILTER ORDER BY id"
$CLICKHOUSE_LOCAL --print-profile-events -q "SELECT id FROM file('$POINTS', Parquet) WHERE $FILTER ORDER BY id FORMAT Null" 2>&1 \
    | grep -o 'ParquetPrunedRowGroups: [0-9]*'
echo "--- the same without pruning"
$CLICKHOUSE_LOCAL -q "SELECT id FROM file('$POINTS', Parquet) WHERE $FILTER ORDER BY id SETTINGS input_format_parquet_spatial_filter_push_down = 0"

echo "--- pruning near the origin"
FILTER="pointInPolygon(geometry, [(-10., -10.), (10., -10.), (10., 10.), (-10., 10.), (-10., -10.)])"
$CLICKHOUSE_LOCAL -q "SELECT id FROM file('$POINTS', Parquet) WHERE $FILTER ORDER BY id"
$CLICKHOUSE_LOCAL --print-profile-events -q "SELECT id FROM file('$POINTS', Parquet) WHERE $FILTER FORMAT Null" 2>&1 \
    | grep -o 'ParquetPrunedRowGroups: [0-9]*'

echo "--- pruning of a file written by ClickHouse: by GeospatialStatistics, the GeoParquet metadata has no covering"
DATA_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"
# 10 row groups, row group N contains polygons around (10 * N, 10 * N).
$CLICKHOUSE_LOCAL -q "
    SELECT number AS id, intDiv(number, 3) * 10 AS c,
        [[(c, c), (c + 1, c), (c + 1, c + 1), (c, c)]]::Polygon AS pg,
        (pg::Geometry) AS geom
    FROM numbers(30)
    INTO OUTFILE '${DATA_DIR}/polygons.parquet' FORMAT Parquet
    SETTINGS output_format_parquet_row_group_size = 3, allow_suspicious_variant_types = 1, output_format_parquet_geometry_logical_type = 1"
$CLICKHOUSE_LOCAL -q "SELECT c.logical_type FROM file('${DATA_DIR}/polygons.parquet', ParquetMetadata) ARRAY JOIN columns AS c WHERE c.name IN ('pg', 'geom')"
for column in pg geom
do
    echo "-- $column"
    FILTER="polygonsIntersectCartesian($column, [[(29.5, 29.5), (30.5, 29.5), (30.5, 30.5), (29.5, 30.5), (29.5, 29.5)]])"
    $CLICKHOUSE_LOCAL -q "SELECT id FROM file('${DATA_DIR}/polygons.parquet', Parquet) WHERE $FILTER ORDER BY id"
    $CLICKHOUSE_LOCAL --print-profile-events -q "SELECT id FROM file('${DATA_DIR}/polygons.parquet', Parquet) WHERE $FILTER FORMAT Null" 2>&1 \
        | grep -o 'ParquetPrunedRowGroups: [0-9]*'
done
rm -rf "${DATA_DIR}"

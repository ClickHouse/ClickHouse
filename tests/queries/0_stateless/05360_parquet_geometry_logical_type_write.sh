#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs `pyarrow`.

# Writing geo columns to Parquet with the `GEOMETRY` logical type and `GeospatialStatistics`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"

QUERY="
SELECT
    number AS id,
    (toFloat64(number), -number / 2)::Point AS pt,
    [(number, 0), (number + 1, 1)]::LineString AS ls,
    [[(0, 0), (number + 1, 0), (number + 1, 1), (0, 0)]]::Polygon AS pg,
    [[(0, number), (1, number)], [(2, 2), (3, 3)]]::MultiLineString AS mls,
    [[[(0, 0), (1, 0), (1, number + 1), (0, 0)]]]::MultiPolygon AS mpg,
    [(number, number), (-1, -1)]::MultiPoint AS mpt,
    multiIf(number = 0, ((1, 2)::Point)::Geometry, number = 1, ([(0, 0), (5, 5)]::LineString)::Geometry, number = 2, NULL::Geometry, ([[(0, 0), (1, 0), (0, 1), (0, 0)]]::Polygon)::Geometry) AS geom
FROM numbers(4)"

echo "--- round trip"
$CLICKHOUSE_LOCAL -q "$QUERY INTO OUTFILE '${DATA_DIR}/geo.parquet' FORMAT Parquet SETTINGS allow_suspicious_variant_types = 1, output_format_parquet_geometry_logical_type = 1"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${DATA_DIR}/geo.parquet', Parquet)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('${DATA_DIR}/geo.parquet', Parquet) ORDER BY id"
$CLICKHOUSE_LOCAL -q "
    SELECT count() FROM
    (
        SELECT * FROM file('${DATA_DIR}/geo.parquet', Parquet)
        EXCEPT
        $QUERY
    ) SETTINGS allow_suspicious_variant_types = 1"

echo "--- ParquetMetadata: logical types"
$CLICKHOUSE_LOCAL -q "
    SELECT c.name, c.physical_type, c.logical_type
    FROM file('${DATA_DIR}/geo.parquet', ParquetMetadata) ARRAY JOIN columns AS c"

echo "--- ParquetMetadata: statistics"
$CLICKHOUSE_LOCAL -q "
    SELECT c.name, c.have_statistics, c.statistics.null_count, c.statistics.min, c.statistics.max,
        c.have_geospatial_statistics, c.geospatial_statistics
    FROM file('${DATA_DIR}/geo.parquet', ParquetMetadata) ARRAY JOIN row_groups AS rg ARRAY JOIN rg.columns AS c"

echo "--- GeoParquet metadata"
python3 - "${DATA_DIR}/geo.parquet" <<'PY'
import json
import sys

import pyarrow.parquet as pq

geo = json.loads(pq.ParquetFile(sys.argv[1]).metadata.metadata[b"geo"])
for name, column in sorted(geo["columns"].items()):
    print(name, column["encoding"], column["geometry_types"], column.get("crs"))
PY

echo "--- pyarrow: logical types, statistics and column index"
python3 - "${DATA_DIR}/geo.parquet" <<'PY'
import sys

import pyarrow.parquet as pq

metadata = pq.ParquetFile(sys.argv[1]).metadata
schema = metadata.schema
for i in range(metadata.num_columns):
    chunk = metadata.row_group(0).column(i)
    geo = chunk.geo_statistics
    stats = chunk.statistics
    print(
        chunk.path_in_schema,
        schema.column(i).logical_type,
        "min_max" if stats is not None and stats.has_min_max else "no_min_max",
        "column_index" if chunk.has_column_index else "no_column_index",
        None if geo is None else (geo.geospatial_types, geo.xmin, geo.xmax, geo.ymin, geo.ymax, geo.zmin, geo.mmin),
    )
PY

echo "--- default: no logical type"
$CLICKHOUSE_LOCAL -q "$QUERY INTO OUTFILE '${DATA_DIR}/old.parquet' FORMAT Parquet SETTINGS allow_suspicious_variant_types = 1"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${DATA_DIR}/old.parquet', Parquet)"
$CLICKHOUSE_LOCAL -q "
    SELECT c.name, c.have_statistics, c.statistics.min IS NOT NULL, c.have_geospatial_statistics
    FROM file('${DATA_DIR}/old.parquet', ParquetMetadata) ARRAY JOIN row_groups AS rg ARRAY JOIN rg.columns AS c"
$CLICKHOUSE_LOCAL -q "
    SELECT c.name, c.logical_type
    FROM file('${DATA_DIR}/old.parquet', ParquetMetadata) ARRAY JOIN columns AS c"
python3 - "${DATA_DIR}/old.parquet" <<'PY'
import json
import sys

import pyarrow.parquet as pq

geo = json.loads(pq.ParquetFile(sys.argv[1]).metadata.metadata[b"geo"])
for name, column in sorted(geo["columns"].items()):
    print(name, column.get("crs"))
PY
echo "--- output_format_parquet_geometadata = 0"
$CLICKHOUSE_LOCAL -q "SELECT (1, 2)::Point AS pt INTO OUTFILE '${DATA_DIR}/tuples.parquet' FORMAT Parquet SETTINGS output_format_parquet_geometadata = 0, output_format_parquet_geometry_logical_type = 1"
$CLICKHOUSE_LOCAL -q "
    SELECT c.name, c.physical_type, c.logical_type
    FROM file('${DATA_DIR}/tuples.parquet', ParquetMetadata) ARRAY JOIN columns AS c"

echo "--- statistics of NULLs and empty geometries"
$CLICKHOUSE_LOCAL -q "
    SELECT number AS id, if(number = 0, ((1, 2)::Point)::Geometry, NULL::Geometry) AS geom, []::LineString AS ls
    FROM numbers(2)
    INTO OUTFILE '${DATA_DIR}/nulls.parquet' FORMAT Parquet
    SETTINGS output_format_parquet_row_group_size = 1, allow_suspicious_variant_types = 1, output_format_parquet_geometry_logical_type = 1"
$CLICKHOUSE_LOCAL -q "
    SELECT c.name, c.statistics.null_count, c.have_geospatial_statistics, c.geospatial_statistics
    FROM file('${DATA_DIR}/nulls.parquet', ParquetMetadata) ARRAY JOIN row_groups AS rg ARRAY JOIN rg.columns AS c
    WHERE c.name != 'id'"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${DATA_DIR}/nulls.parquet', Parquet)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('${DATA_DIR}/nulls.parquet', Parquet) ORDER BY id"

rm -rf "${DATA_DIR}"

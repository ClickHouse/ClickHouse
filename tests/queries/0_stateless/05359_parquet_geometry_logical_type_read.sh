#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs `pyarrow`.

# Reading Parquet columns with the `GEOMETRY` and `GEOGRAPHY` logical types written by another
# implementation (Arrow C++ via `pyarrow`), without the GeoParquet `geo` metadata.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"
FILE="${DATA_DIR}/native.parquet"

# The `geoarrow.wkb` extension type makes Arrow write the `GEOMETRY` logical type, or `GEOGRAPHY`
# if the extension metadata has `edges`. Arrow writes `GeospatialStatistics` and no min/max.
python3 - "${FILE}" <<'PY'
import struct
import sys

import pyarrow as pa
import pyarrow.parquet as pq


class WKB(pa.ExtensionType):
    def __init__(self, metadata=b"{}"):
        self._metadata = metadata
        super().__init__(pa.binary(), "geoarrow.wkb")

    def __arrow_ext_serialize__(self):
        return self._metadata

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return WKB(serialized)


def point(x, y):
    return struct.pack("<BIdd", 1, 1, x, y)


def points(pts):
    return struct.pack("<I", len(pts)) + b"".join(struct.pack("<dd", x, y) for x, y in pts)


def linestring(pts):
    return struct.pack("<BI", 1, 2) + points(pts)


def polygon(rings):
    return struct.pack("<BII", 1, 3, len(rings)) + b"".join(points(r) for r in rings)


def multipolygon(polygons):
    return struct.pack("<BII", 1, 6, len(polygons)) + b"".join(polygon(p) for p in polygons)


square = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]


def column(values, metadata=b"{}"):
    return pa.ExtensionArray.from_storage(WKB(metadata), pa.array(values, pa.binary()))


table = pa.table({
    "id": pa.array([1, 2, 3], pa.int32()),
    # Only points: read as `Point`, because `GeospatialStatistics` list a single type.
    "pt": column([point(1, 2), point(3, 4), point(-5.5, 6.25)]),
    # Several types: read as `Geometry`.
    "mixed": column([point(1, 2), linestring([(0, 0), (1, 1)]), multipolygon([[square]])]),
    # `GEOGRAPHY` with the default (spherical) edges: read into the same types.
    "geog": column([polygon([square]), polygon([square]), polygon([square])], b'{"edges": "spherical"}'),
    # NULLs: geo types cannot be inside `Nullable`, so a NULL becomes the default value.
    "pt_null": column([point(7, 8), None, point(9, 10)]),
    "mixed_null": column([None, linestring([(0, 0), (1, 1)]), point(9, 10)]),
})
pq.write_table(table, sys.argv[1], store_schema=False)

schema = pq.ParquetFile(sys.argv[1]).schema
for name in ["pt", "mixed", "geog", "pt_null"]:
    print(name, schema.column(schema.names.index(name)).logical_type)
PY

echo "--- DESCRIBE"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${FILE}', Parquet)"

echo "--- data"
$CLICKHOUSE_LOCAL -q "SELECT id, pt, mixed, variantType(mixed), geog FROM file('${FILE}', Parquet) ORDER BY id"

echo "--- NULLs"
$CLICKHOUSE_LOCAL -q "SELECT id, pt_null, mixed_null, variantType(mixed_null) FROM file('${FILE}', Parquet) ORDER BY id"

echo "--- String type hint returns raw WKB"
$CLICKHOUSE_LOCAL -q "SELECT id, hex(pt), length(mixed) FROM file('${FILE}', Parquet, 'id Int32, pt String, mixed String') ORDER BY id"

echo "--- explicit geo type hint"
$CLICKHOUSE_LOCAL -q "SELECT id, pt, variantType(pt), geog FROM file('${FILE}', Parquet, 'id Int32, pt Geometry, geog Geometry') ORDER BY id"
$CLICKHOUSE_LOCAL -q "SELECT id, geog FROM file('${FILE}', Parquet, 'id Int32, geog Polygon') ORDER BY id"
echo "--- a value of another type than requested"
$CLICKHOUSE_LOCAL -q "SELECT mixed FROM file('${FILE}', Parquet, 'mixed Point')" 2>&1 | grep -o -m1 'Types in parquet mismatched'

echo "--- input_format_parquet_allow_geoparquet_parser = 0"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${FILE}', Parquet) SETTINGS input_format_parquet_allow_geoparquet_parser = 0"
$CLICKHOUSE_LOCAL -q "SELECT id, hex(pt) FROM file('${FILE}', Parquet) ORDER BY id SETTINGS input_format_parquet_allow_geoparquet_parser = 0"
echo "--- input_format_parquet_allow_geoparquet_parser = 0 with an explicit geo type"
$CLICKHOUSE_LOCAL -q "SELECT id, pt FROM file('${FILE}', Parquet, 'id Int32, pt Point') ORDER BY id SETTINGS input_format_parquet_allow_geoparquet_parser = 0"

echo "--- spatial functions"
$CLICKHOUSE_LOCAL -q "SELECT id, abs(round(areaCartesian(geog), 1)) FROM file('${FILE}', Parquet) ORDER BY id"

echo "--- geometries with Z coordinates are returned as raw WKB"
FILE_Z="${DATA_DIR}/z.parquet"
python3 - "${FILE_Z}" <<'PY'
import struct
import sys

import pyarrow as pa
import pyarrow.parquet as pq


class WKB(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.binary(), "geoarrow.wkb")

    def __arrow_ext_serialize__(self):
        return b"{}"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return WKB()


# `Point Z`: ISO WKB code 1001.
values = [struct.pack("<BIddd", 1, 1001, 1, 2, 3), struct.pack("<BIddd", 1, 1001, 4, 5, 6)]
table = pa.table({
    "id": pa.array([1, 2], pa.int32()),
    "pt_z": pa.ExtensionArray.from_storage(WKB(), pa.array(values, pa.binary())),
})
pq.write_table(table, sys.argv[1], store_schema=False)
print(pq.ParquetFile(sys.argv[1]).metadata.row_group(0).column(1).geo_statistics.geospatial_types)
PY
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${FILE_Z}', Parquet)"
$CLICKHOUSE_LOCAL -q "SELECT id, hex(pt_z) FROM file('${FILE_Z}', Parquet) ORDER BY id"

rm -rf "${DATA_DIR}"

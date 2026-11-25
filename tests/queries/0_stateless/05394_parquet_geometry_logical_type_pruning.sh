#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs `pyarrow`.

# Spatial row group pruning by `GeospatialStatistics` must not change the result:
# - a bounding box crossing the antimeridian (`xmin > xmax`) bounds the vertices of a polygon, but
#   not its straight segments, which the Cartesian predicates use; the same for a multipoint
#   used as a polygon;
# - a NULL read into a non-nullable geo type becomes the default value, e.g. the point (0, 0),
#   which is not covered by the bounding box;
# - a conversion of coordinates to the requested type, e.g. `Tuple(UInt8, UInt8)`, can move a
#   geometry out of the bounding box.
# Also, an explicitly requested `Nullable(Point)` keeps NULLs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"

python3 - "${DATA_DIR}" <<'PY'
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


def polygon(ring):
    return struct.pack("<BIII", 1, 3, 1, len(ring)) + b"".join(struct.pack("<dd", x, y) for x, y in ring)


def multipoint(points):
    return struct.pack("<BII", 1, 4, len(points)) + b"".join(point(x, y) for x, y in points)


def column(values):
    return pa.ExtensionArray.from_storage(WKB(), pa.array(values, pa.binary()))


# Arrow writes the bounding box `xmin = -170, xmax = 170`. Swap the two values in the footer to
# get the bounding box `xmin = 170, xmax = -170` crossing the antimeridian, as for a `GEOGRAPHY`.
def write_wrapped(path, value):
    pq.write_table(pa.table({"geometry": column([value])}), path, store_schema=False)
    data = bytearray(open(path, "rb").read())
    footer_length = struct.unpack("<I", data[-8:-4])[0]
    footer_start = len(data) - 8 - footer_length
    footer = bytes(data[footer_start:-8])
    a, b = struct.pack("<d", -170.), struct.pack("<d", 170.)
    assert footer.count(a) == 1 and footer.count(b) == 1
    footer = footer.replace(a, b"\0" * 8).replace(b, a).replace(b"\0" * 8, b)
    data[footer_start:-8] = footer
    open(path, "wb").write(bytes(data))
    bbox = pq.ParquetFile(path).metadata.row_group(0).column(0).geo_statistics
    print("wrapped bbox:", bbox.xmin, bbox.xmax)


ring = [(170., -10.), (-170., -10.), (-170., 10.), (170., 10.), (170., -10.)]
write_wrapped(sys.argv[1] + "/wrapped.parquet", polygon(ring))
write_wrapped(sys.argv[1] + "/wrapped_multipoint.parquet", multipoint(ring[:4]))

pq.write_table(pa.table({"pt": column([point(7, 8), None, point(9, 10)])}), sys.argv[1] + "/nulls.parquet", store_schema=False)
pq.write_table(pa.table({"pt": column([point(1.9, 1.9)])}), sys.argv[1] + "/point.parquet", store_schema=False)
PY

echo "--- a polygon with a bounding box crossing the antimeridian"
FILTER="polygonsIntersectCartesian(geometry, [[(-1., -1.), (1., -1.), (1., 1.), (-1., 1.), (-1., -1.)]])"
for push_down in 0 1
do
    $CLICKHOUSE_LOCAL -q "SELECT count() FROM file('${DATA_DIR}/wrapped.parquet', Parquet) WHERE $FILTER SETTINGS input_format_parquet_spatial_filter_push_down = $push_down"
done

echo "--- a multipoint with a bounding box crossing the antimeridian, used as a polygon"
FILTER="pointInPolygon((1., 1.), geometry)"
for push_down in 0 1
do
    $CLICKHOUSE_LOCAL -q "SELECT count() FROM file('${DATA_DIR}/wrapped_multipoint.parquet', Parquet) WHERE $FILTER SETTINGS input_format_parquet_spatial_filter_push_down = $push_down"
done

echo "--- NULL points read as (0, 0)"
FILTER="pointInPolygon(pt, [(-1., -1.), (1., -1.), (1., 1.), (-1., 1.), (-1., -1.)])"
for push_down in 0 1
do
    $CLICKHOUSE_LOCAL -q "SELECT pt FROM file('${DATA_DIR}/nulls.parquet', Parquet) WHERE $FILTER SETTINGS input_format_parquet_spatial_filter_push_down = $push_down"
done
echo "--- pruning still works if the filter does not contain (0, 0)"
FILTER="pointInPolygon(pt, [(20., 20.), (21., 20.), (21., 21.), (20., 21.), (20., 20.)])"
$CLICKHOUSE_LOCAL --print-profile-events -q "SELECT pt FROM file('${DATA_DIR}/nulls.parquet', Parquet) WHERE $FILTER FORMAT Null" 2>&1 \
    | grep -o 'ParquetPrunedRowGroups: [0-9]*'

echo "--- coordinates converted to the requested type"
FILTER="pointInPolygon(pt, [(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5), (0.5, 0.5)])"
for push_down in 0 1
do
    $CLICKHOUSE_LOCAL -q "SELECT pt FROM file('${DATA_DIR}/point.parquet', Parquet, 'pt Tuple(UInt8, UInt8)') WHERE $FILTER SETTINGS input_format_parquet_spatial_filter_push_down = $push_down"
done

echo "--- NULLs read as Nullable(Point)"
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(pt), pt, isNull(pt) FROM file('${DATA_DIR}/nulls.parquet', Parquet, 'pt Nullable(Point)')"

rm -rf "${DATA_DIR}"

#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for heap-buffer-overflow in readColumnWithGeoData when an
# Arrow file carries corrupted intermediate offsets, and for the crash caused
# by using buffer->mutable_data() (which returns nullptr for read-only IPC
# buffers) instead of buffer->data().

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_geo.arrow"
DATA_FILE_VALID="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_geo_valid.arrow"
trap 'rm -f "$DATA_FILE" "$DATA_FILE_VALID"' EXIT

# Build a valid Arrow IPC file with WKB Point rows and geo schema metadata.
# Before the fix, buffer->mutable_data() returned nullptr for read-only IPC
# buffers, causing a null-pointer dereference on any geo-tagged Arrow column.
python3 - "$DATA_FILE_VALID" <<'EOF'
import struct
import sys
import json
import pyarrow as pa
import pyarrow.ipc as ipc

path = sys.argv[1]

# WKB Point(1.0, 2.0): byte_order=1(LE), type=1(Point), x=1.0, y=2.0
wkb_point = struct.pack('<BIdd', 1, 1, 1.0, 2.0)

arr = pa.array([wkb_point, wkb_point], type=pa.binary())
tbl = pa.Table.from_arrays([arr], names=['x'])
geo_meta = json.dumps({'columns': {'x': {'encoding': 'WKB', 'geometry_types': ['Point']}}})

# Geo metadata must be attached to the schema (schema->metadata()), not to the
# IPC footer (file_reader->metadata()), which is what ArrowBlockInputFormat reads.
meta = tbl.schema.metadata or {}
meta[b'geo'] = geo_meta.encode()
tbl = tbl.replace_schema_metadata(meta)

with pa.OSFile(path, 'wb') as f:
    w = ipc.new_file(f, tbl.schema)
    w.write_table(tbl)
    w.close()
EOF

$CLICKHOUSE_LOCAL --input_format_parquet_allow_geoparquet_parser=1 \
    --query "SELECT x FROM file('${DATA_FILE_VALID}', 'Arrow')" 2>&1

# Build an Arrow IPC file with WKB binary rows, geo schema metadata, and a
# corrupted offset that makes value_length(0) = 1 GiB.
# Before the fix, this caused a heap-buffer-overflow in parseWKBFormat.
# After the fix, it must be rejected with INCORRECT_DATA.
python3 - "$DATA_FILE" <<'EOF'
import struct
import sys
import json
import pyarrow as pa
import pyarrow.ipc as ipc

path = sys.argv[1]

wkb_point = struct.pack('<BIdd', 1, 1, 1.0, 2.0)  # 21 bytes

arr = pa.array([wkb_point, wkb_point, wkb_point], type=pa.binary())
tbl = pa.Table.from_arrays([arr], names=['x'])
geo_meta = json.dumps({'columns': {'x': {'encoding': 'WKB', 'geometry_types': ['Point']}}})

meta = tbl.schema.metadata or {}
meta[b'geo'] = geo_meta.encode()
tbl = tbl.replace_schema_metadata(meta)

with pa.OSFile(path, 'wb') as f:
    w = ipc.new_file(f, tbl.schema)
    w.write_table(tbl)
    w.close()

# Patch offsets [0, 21, 42, 63] -> [0, 0x40000000, 42, 63].
# value_length(0) = offset[1] - offset[0] = 1 GiB; the read would overflow
# the ~63-byte data buffer.
data = bytearray(open(path, 'rb').read())
needle = struct.pack('<IIII', 0, 21, 42, 63)
idx = data.find(needle)
assert idx >= 0, "could not locate offsets array"
data[idx + 4 : idx + 8] = struct.pack('<I', 0x40000000)
open(path, 'wb').write(bytes(data))
EOF

$CLICKHOUSE_LOCAL --input_format_parquet_allow_geoparquet_parser=1 \
    --query "SELECT x FROM file('${DATA_FILE}', 'Arrow')" 2>&1 \
    | grep -oF 'INCORRECT_DATA' || echo 'FAIL: expected INCORRECT_DATA'

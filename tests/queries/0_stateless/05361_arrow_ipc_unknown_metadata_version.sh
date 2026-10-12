#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow IPC inputs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# `Message.version` and `Footer.version` name the edition of the Arrow IPC specification the writer used.
# A reader cannot interpret an edition it does not implement, because the buffer layout differs between
# editions, so it must reject such an input instead of decoding it under the wrong layout. No writer can
# produce a version that does not exist, so the inputs below are written by pyarrow and then patched in
# the one field that holds the version.

python3 - "$TMP_DIR" <<'PYEOF'
import struct
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
table = pa.table({"x": pa.array([1, 2, 3], pa.int32())})


def set_version(buf, flatbuffer_start, value):
    """Overwrite the `version` field (field 0) of the FlatBuffer table starting at `flatbuffer_start`."""
    root = flatbuffer_start + struct.unpack_from("<I", buf, flatbuffer_start)[0]
    vtable = root - struct.unpack_from("<i", buf, root)[0]
    vtable_length = struct.unpack_from("<H", buf, vtable)[0]
    assert vtable_length >= 6, "the vtable has no slot for field 0"
    slot = struct.unpack_from("<H", buf, vtable + 4)[0]
    assert slot != 0, "the version field is absent"
    patched = bytearray(buf)
    struct.pack_into("<h", patched, root + slot, value)
    return bytes(patched)


# The enum counts from zero, so V5 is 4 and 5 is a version that does not exist.
V6 = 5
# V1 is the lowest value the enum has, and this reader implements V4 and later.
V1 = 0


def to_stream():
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def to_file():
    sink = pa.BufferOutputStream()
    with ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


# A stream starts with its schema message: a 4-byte continuation token, a 4-byte metadata length, then
# the Message FlatBuffer.
stream = to_stream()
with open(f"{out}/future.arrows", "wb") as f:
    f.write(set_version(stream, 8, V6))
with open(f"{out}/legacy.arrows", "wb") as f:
    f.write(set_version(stream, 8, V1))

# A file ends with its footer, then a 4-byte footer length, then the 6 magic bytes.
arrow_file = to_file()
footer_length = struct.unpack_from("<i", arrow_file, len(arrow_file) - 10)[0]
with open(f"{out}/future_footer.arrow", "wb") as f:
    f.write(set_version(arrow_file, len(arrow_file) - 10 - footer_length, V6))
PYEOF

echo "-- a stream whose schema message declares a version that does not exist"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$TMP_DIR/future.arrows', ArrowStream)" 2>&1 |
    grep -c "Unsupported future Arrow IPC metadata version V6"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$TMP_DIR/future.arrows', ArrowStream)" 2>&1 |
    grep -c "Unsupported future Arrow IPC metadata version V6"

echo "-- a stream whose schema message declares a version this reader does not implement"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$TMP_DIR/legacy.arrows', ArrowStream)" 2>&1 |
    grep -c "Unsupported legacy Arrow IPC metadata version V1"

echo "-- a file whose footer declares a version that does not exist"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$TMP_DIR/future_footer.arrow', Arrow)" 2>&1 |
    grep -c "Unsupported Arrow IPC metadata version V6 in the Arrow file footer"

echo "-- a stream and a file ClickHouse writes itself still read back"
$CLICKHOUSE_LOCAL --multiquery "
    INSERT INTO FUNCTION file('$TMP_DIR/roundtrip.arrows', ArrowStream, 'number UInt64') SELECT number FROM numbers(4);
    INSERT INTO FUNCTION file('$TMP_DIR/roundtrip.arrow', Arrow, 'number UInt64') SELECT number FROM numbers(4);
    SELECT sum(number) FROM file('$TMP_DIR/roundtrip.arrows', ArrowStream);
    SELECT sum(number) FROM file('$TMP_DIR/roundtrip.arrow', Arrow);
"

echo "-- a stream pyarrow writes at the lowest version this reader implements still reads back"
python3 -c "
import pyarrow as pa, pyarrow.ipc as ipc, sys
table = pa.table({'x': pa.array([1, 2, 3], pa.int32())})
options = ipc.IpcWriteOptions(metadata_version=ipc.MetadataVersion.V4)
with ipc.new_stream('$TMP_DIR/v4.arrows', table.schema, options=options) as writer:
    writer.write_table(table)
"
$CLICKHOUSE_LOCAL -q "SELECT sum(x) FROM file('$TMP_DIR/v4.arrows', ArrowStream)"

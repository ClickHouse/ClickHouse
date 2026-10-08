#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for the native Arrow IPC reader: `Message.bodyLength` is untrusted. Before the fix the
# reader allocated the full declared body length up front, so a tiny stream declaring a huge body would
# attempt an enormous allocation. The reader must instead materialize only the bytes the record batch
# buffers actually reference and skip the rest, so an over-long declared body fails cleanly as
# CANNOT_READ_ALL_DATA (when the tail is absent) rather than driving a giant allocation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_body.arrows"
trap 'rm -f "$DATA_FILE"' EXIT

# Build a valid ArrowStream with one small Int64 column, then patch the record batch message's
# `bodyLength` flatbuffer field to 1 PiB while leaving the actual (tiny) body in place. The vtable is
# decoded so the exact int64 field is patched (the setup asserts it matched the real body length first).
python3 - "$DATA_FILE" <<'EOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

path = sys.argv[1]

tbl = pa.Table.from_arrays([pa.array([1, 2, 3, 4, 5], type=pa.int64())], names=['x'])

sink = pa.BufferOutputStream()
w = ipc.new_stream(sink, tbl.schema)
w.write_table(tbl)
w.close()
data = bytearray(sink.getvalue().to_pybytes())

# Find the record batch message: its flatbuffer metadata bytes and the real body length.
rb_meta = None
rb_body_len = None
reader = ipc.MessageReader.open_stream(pa.BufferReader(bytes(data)))
for message in reader:
    if message.type == 'record batch':
        rb_meta = message.metadata.to_pybytes()
        rb_body_len = message.body.size
        break
assert rb_meta is not None, "no record batch message found"

# Locate the flatbuffer in the stream (the framing prefix is not part of message.metadata).
mi = data.find(rb_meta)
assert mi >= 0, "could not locate record batch metadata in the stream"

# Decode the FlatBuffer Message vtable to find the `bodyLength` field. Message fields:
# 0=version, 1=header_type, 2=header, 3=bodyLength, 4=custom_metadata (a union takes two slots).
root = int.from_bytes(rb_meta[0:4], 'little')
soffset = int.from_bytes(rb_meta[root:root + 4], 'little', signed=True)
vtable = root - soffset
vt_size = int.from_bytes(rb_meta[vtable:vtable + 2], 'little')
slot = vtable + 4 + 2 * 3
field_off = int.from_bytes(rb_meta[slot:slot + 2], 'little') if slot + 2 <= vtable + vt_size else 0
assert field_off != 0, "bodyLength field not present in the vtable"
body_field_pos = root + field_off
cur = int.from_bytes(rb_meta[body_field_pos:body_field_pos + 8], 'little', signed=True)
assert cur == rb_body_len, f"vtable decode mismatch: {cur} != {rb_body_len}"

huge = 1 << 50  # 1 PiB
file_pos = mi + body_field_pos
data[file_pos:file_pos + 8] = huge.to_bytes(8, 'little', signed=True)

open(path, 'wb').write(bytes(data))
EOF

$CLICKHOUSE_LOCAL --input_format_arrow_use_native_reader=1 \
    --query "SELECT * FROM file('${DATA_FILE}', 'ArrowStream')" 2>&1 \
    | grep -oF 'CANNOT_READ_ALL_DATA' | head -1 || echo 'FAIL: expected CANNOT_READ_ALL_DATA'

#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for native Arrow IPC subset reads: a SELECT of a subset of columns must not read,
# validate, or fail on the buffers of columns it did not request. Here the unrequested column's last
# buffer is patched to an out-of-bounds (offset+length past the body) range. Selecting only the intact
# column must succeed; selecting the corrupt column must fail. Before the subset-read fix the reader
# validated and materialized every buffer up front, so the corrupt unrequested column failed the read
# even when the requested column was intact.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for FORMAT in ArrowStream Arrow; do
    DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${FORMAT}.arrow"

    python3 - "$DATA_FILE" "$FORMAT" <<'EOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

path, fmt = sys.argv[1], sys.argv[2]

# Two columns; `big` (a String) is written after `small`, so its data buffer is the last buffer of the
# record batch. Only `big`'s buffer is corrupted below.
tbl = pa.table({
    'small': pa.array([1, 2, 3, 4, 5], type=pa.int64()),
    'big': pa.array(['v%d' % i for i in range(5)], type=pa.string()),
})

def build(writer_factory):
    sink = pa.BufferOutputStream()
    w = writer_factory(sink, tbl.schema)
    w.write_table(tbl)
    w.close()
    return bytearray(sink.getvalue().to_pybytes())

# The record-batch metadata FlatBuffer is identical regardless of stream/file framing (buffer offsets are
# relative to the message body), so extract it from a stream copy and locate it in the target bytes.
def record_batch_metadata():
    sink = pa.BufferOutputStream()
    w = ipc.new_stream(sink, tbl.schema)
    w.write_table(tbl)
    w.close()
    reader = ipc.MessageReader.open_stream(pa.BufferReader(sink.getvalue()))
    for message in reader:
        if message.type == 'record batch':
            return message.metadata.to_pybytes()
    raise AssertionError("no record batch message found")

meta = record_batch_metadata()

def field_pos(table_pos, field_index):
    """Absolute position of a table field's value (0 if absent)."""
    soffset = int.from_bytes(meta[table_pos:table_pos + 4], 'little', signed=True)
    vtable = table_pos - soffset
    vt_size = int.from_bytes(meta[vtable:vtable + 2], 'little')
    slot = vtable + 4 + 2 * field_index
    if slot + 2 > vtable + vt_size:
        return 0
    field_off = int.from_bytes(meta[slot:slot + 2], 'little')
    return table_pos + field_off if field_off else 0

def follow_uoffset(pos):
    return pos + int.from_bytes(meta[pos:pos + 4], 'little')

# Message root -> header (field 2 = RecordBatch table) -> buffers (field 2 = vector of Buffer{offset,length}).
root = int.from_bytes(meta[0:4], 'little')
rb = follow_uoffset(field_pos(root, 2))
buffers_vec = follow_uoffset(field_pos(rb, 2))
count = int.from_bytes(meta[buffers_vec:buffers_vec + 4], 'little')
assert count >= 1, "record batch has no buffers"

# Patch the last buffer (`big`'s data buffer): a Buffer is a 16-byte struct {int64 offset, int64 length};
# set its length so offset+length runs past the message body.
last_length_off = buffers_vec + 4 + (count - 1) * 16 + 8

data = build(ipc.new_stream if fmt == 'ArrowStream' else ipc.new_file)
mi = data.find(meta)
assert mi >= 0, "could not locate record batch metadata in the %s output" % fmt
file_pos = mi + last_length_off
data[file_pos:file_pos + 8] = (1 << 50).to_bytes(8, 'little', signed=True)  # 1 PiB length

open(path, 'wb').write(bytes(data))
EOF

    echo "--- ${FORMAT}: subset SELECT of the intact column succeeds ---"
    ${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=1 \
        --query "SELECT small FROM file('${DATA_FILE}', '${FORMAT}') ORDER BY small"

    echo "--- ${FORMAT}: SELECT of the corrupt column fails ---"
    ${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=1 \
        --query "SELECT big FROM file('${DATA_FILE}', '${FORMAT}') FORMAT Null" 2>&1 \
        | grep -oF 'out of the message body' | head -1 || echo 'FAIL: expected an out-of-bounds error'

    rm -f "$DATA_FILE"
done

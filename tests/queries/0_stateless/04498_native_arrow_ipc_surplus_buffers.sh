#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for the native Arrow IPC reader: the exact FieldNode/buffer-consumption check must run for
# fully decoded (non-pruned) record batches too, not only when columns are skipped. A batch declaring more
# field nodes/buffers than the schema's fields consume could otherwise make the decoder read a column from an
# interposed buffer and silently ignore the real trailing one. Here a two-column batch is written and the
# schema's field vector is patched down to a single field, so the record batch carries surplus field nodes
# and buffers that the decode of the one remaining column does not consume.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrows"
trap 'rm -f "$DATA_FILE"' EXIT

python3 - "$DATA_FILE" <<'EOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

path = sys.argv[1]

# Two int32 columns: the record batch declares two field nodes and four buffers (validity + values each).
schema = pa.schema([pa.field('a', pa.int32()), pa.field('b', pa.int32())])
tbl = pa.table([pa.array([1, 2, 3], type=pa.int32()), pa.array([4, 5, 6], type=pa.int32())], schema=schema)

sink = pa.BufferOutputStream()
w = ipc.new_stream(sink, schema)
w.write_table(tbl)
w.close()
data = bytearray(sink.getvalue().to_pybytes())

meta = None
for message in ipc.MessageReader.open_stream(pa.BufferReader(bytes(data))):
    if message.type == 'schema':
        meta = message.metadata.to_pybytes()
        break
assert meta is not None, "no schema message found"

def field_pos(table_pos, field_index):
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

# Message root -> header (field 2 = Schema) -> fields (field 1 = vector of Field).
root = int.from_bytes(meta[0:4], 'little')
schema_table = follow_uoffset(field_pos(root, 2))
fields_vec = follow_uoffset(field_pos(schema_table, 1))
count = int.from_bytes(meta[fields_vec:fields_vec + 4], 'little')
assert count == 2, "expected two fields, got %d" % count

# Drop field 'b' from the schema view: the reader now sees one column while the record batch still carries
# both columns' field nodes and buffers.
patched = bytearray(meta)
patched[fields_vec:fields_vec + 4] = (1).to_bytes(4, 'little')

mi = data.find(meta)
assert mi >= 0, "could not locate the schema message"
data[mi:mi + len(meta)] = patched

open(path, 'wb').write(bytes(data))
EOF

echo "--- record batch with surplus field nodes/buffers is rejected ---"
${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=1 \
    --query "SELECT * FROM file('${DATA_FILE}', 'ArrowStream') FORMAT Null" 2>&1 \
    | grep -oF 'does not match the schema' | head -1 || echo 'FAIL: expected the surplus record batch layout to be rejected'

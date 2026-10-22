#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for the native Arrow IPC reader: Arrow dictionary ids are global, so two fields reusing
# one id must describe the same dictionary value type. Field metadata changes the mapped ClickHouse type
# for the same physical Arrow type, so it is part of the value type. Here two dictionary-encoded columns
# are made to share a dictionary id (by patching the schema) while carrying different field metadata; the
# reader must reject the reuse instead of silently decoding both columns from one dictionary.

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

# Two dictionary-encoded string columns; only field `a` carries field metadata. pyarrow assigns dictionary
# id 0 to `a` (the flatbuffer default, omitted) and id 1 to `b`; `b`'s id is patched to 0 below so both
# fields reference the same dictionary while their metadata differs.
schema = pa.schema([
    pa.field('a', pa.dictionary(pa.int32(), pa.string()), metadata={'k': 'v'}),
    pa.field('b', pa.dictionary(pa.int32(), pa.string())),
])
tbl = pa.table([pa.array(['a0', 'a1', 'a2']).dictionary_encode(),
                pa.array(['b0', 'b1', 'b2']).dictionary_encode()], schema=schema)

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

# Field `b` is index 1; its DictionaryEncoding is Field field 4, whose id is field 0 (a long, value 1).
b_field = follow_uoffset(fields_vec + 4 + 4 * 1)
b_dict = follow_uoffset(field_pos(b_field, 4))
b_id_pos = field_pos(b_dict, 0)
assert b_id_pos != 0, "field b has no dictionary id"

patched = bytearray(meta)
patched[b_id_pos:b_id_pos + 8] = (0).to_bytes(8, 'little', signed=True)  # collide with field a's id (0)

mi = data.find(meta)
assert mi >= 0, "could not locate the schema message"
data[mi:mi + len(meta)] = patched

open(path, 'wb').write(bytes(data))
EOF

echo "--- shared dictionary id with differing field metadata is rejected ---"
${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=1 \
    --query "SELECT a, b FROM file('${DATA_FILE}', 'ArrowStream') FORMAT Null" 2>&1 \
    | grep -oF 'different value types' | head -1 || echo 'FAIL: expected the reused dictionary id to be rejected'

#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for native Arrow IPC subset reads: a SELECT of a subset of columns must not validate or
# fail on the DictionaryBatch of a column it did not request. Here the unrequested column's dictionary
# batch has its `data` field removed (so `DictionaryBatch.data()` is absent). Selecting only the other
# column must still succeed (its dictionary is intact and the corrupt one is skipped); selecting the
# corrupt column must fail. Before the fix the reader validated `data` before checking the dictionary id
# against the requested columns, so a projected read failed on a dictionary it never uses.

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

# Two dictionary-encoded columns. pyarrow assigns dictionary ids in field order, so `a` -> id 0 and
# `b` -> id 1; the `data` field of `b`'s dictionary batch (id 1) is removed below.
tbl = pa.table({
    'a': pa.array(['a0', 'a1', 'a2']).dictionary_encode(),
    'b': pa.array(['b0', 'b1', 'b2']).dictionary_encode(),
})

def build(writer_factory):
    sink = pa.BufferOutputStream()
    w = writer_factory(sink, tbl.schema)
    w.write_table(tbl)
    w.close()
    return bytearray(sink.getvalue().to_pybytes())

def field_pos(meta, table_pos, field_index):
    soffset = int.from_bytes(meta[table_pos:table_pos + 4], 'little', signed=True)
    vtable = table_pos - soffset
    vt_size = int.from_bytes(meta[vtable:vtable + 2], 'little')
    slot = vtable + 4 + 2 * field_index
    if slot + 2 > vtable + vt_size:
        return 0
    field_off = int.from_bytes(meta[slot:slot + 2], 'little')
    return table_pos + field_off if field_off else 0

def follow_uoffset(meta, pos):
    return pos + int.from_bytes(meta[pos:pos + 4], 'little')

# Message root -> header (field 2 = DictionaryBatch table). DictionaryBatch.id is field 0, data is field 1.
def dictionary_batch_table(meta):
    root = int.from_bytes(meta[0:4], 'little')
    return follow_uoffset(meta, field_pos(meta, root, 2))

def dictionary_batch_id(meta):
    dt = dictionary_batch_table(meta)
    p = field_pos(meta, dt, 0)
    return int.from_bytes(meta[p:p + 8], 'little', signed=True) if p else -1

# Metadata of the dictionary batch with the given id, extracted from a stream copy (identical bytes in
# both framings: buffer offsets are relative to the message body).
def dictionary_batch_metadata(want_id):
    sink = pa.BufferOutputStream()
    w = ipc.new_stream(sink, tbl.schema)
    w.write_table(tbl)
    w.close()
    for message in ipc.MessageReader.open_stream(pa.BufferReader(sink.getvalue())):
        if message.type == 'dictionary':
            meta = message.metadata.to_pybytes()
            if dictionary_batch_id(meta) == want_id:
                return meta
    raise AssertionError("dictionary batch with id %d not found" % want_id)

def removed_data_metadata(meta):
    """A copy of the dictionary-batch metadata with the `data` field cleared in its vtable."""
    m = bytearray(meta)
    dt = dictionary_batch_table(m)
    soffset = int.from_bytes(m[dt:dt + 4], 'little', signed=True)
    vtable = dt - soffset
    data_slot = vtable + 4 + 2 * 1  # field index 1 = data
    m[data_slot:data_slot + 2] = (0).to_bytes(2, 'little')  # absent
    return bytes(m)

meta = dictionary_batch_metadata(1)
patched = removed_data_metadata(meta)

data = build(ipc.new_stream if fmt == 'ArrowStream' else ipc.new_file)
mi = data.find(meta)
assert mi >= 0, "could not locate the id-1 dictionary batch in the %s output" % fmt
data[mi:mi + len(meta)] = patched

open(path, 'wb').write(bytes(data))
EOF

    echo "--- ${FORMAT}: SELECT the column whose dictionary is intact succeeds ---"
    ${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=1 \
        --query "SELECT a FROM file('${DATA_FILE}', '${FORMAT}') ORDER BY a"

    echo "--- ${FORMAT}: SELECT the column whose dictionary is corrupt fails ---"
    ${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=1 \
        --query "SELECT b FROM file('${DATA_FILE}', '${FORMAT}') FORMAT Null" 2>&1 \
        | grep -oF 'has no data' | head -1 || echo 'FAIL: expected a missing-data error'

    rm -f "$DATA_FILE"
done

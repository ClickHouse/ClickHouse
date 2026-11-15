#!/usr/bin/env bash
# Tags: no-fasttest
# This test requires PyArrow to write dictionary batches and verify relocated file blocks.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PYEOF'
from pathlib import Path
import struct
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

out = Path(sys.argv[1])


def indirect(data, position):
    return position + struct.unpack_from("<I", data, position)[0]


def field(data, table, index):
    vtable = table - struct.unpack_from("<i", data, table)[0]
    return table + struct.unpack_from("<H", data, vtable + 4 + 2 * index)[0]


def file_blocks(data):
    footer_size = struct.unpack_from("<i", data, len(data) - 10)[0]
    footer_start = len(data) - 10 - footer_size
    footer = indirect(data, footer_start)
    blocks = []
    for field_index in (2, 3):
        vector = indirect(data, field(data, footer, field_index))
        count = struct.unpack_from("<I", data, vector)[0]
        blocks.append([vector + 4 + 24 * i for i in range(count)])
    return footer_start, *blocks


def dictionary(values, indices):
    return pa.DictionaryArray.from_arrays(pa.array(indices, type=pa.int32()), pa.array(values, type=pa.string()))


batches = [
    pa.record_batch([
        dictionary(["alpha", "beta"], [0, 1]),
        dictionary(["left", "right"], [1, 0]),
    ], names=["a", "b"]),
    pa.record_batch([
        dictionary(["alpha", "beta", "gamma"], [2, 0]),
        dictionary(["left", "right", "center"], [0, 2]),
    ], names=["a", "b"]),
]
with ipc.new_file(out / "interleaved.arrow", batches[0].schema,
                  options=ipc.IpcWriteOptions(emit_dictionary_deltas=True)) as writer:
    for batch in batches:
        writer.write_batch(batch)

original = (out / "interleaved.arrow").read_bytes()
footer_start, dictionaries, records = file_blocks(original)
expected = pa.Table.from_batches(batches).to_pydict()

# File dictionaries apply to all record batches. Footer order determines delta order even when the
# physical dictionary blocks are reversed, and record batches may precede their base dictionaries.
for name, order in (
    ("records_first", records + dictionaries),
    ("reversed_dictionaries", records + dictionaries[::-1]),
):
    first_offset = min(struct.unpack_from("<q", original, entry)[0] for entry in order)
    relocated = bytearray(original[:first_offset])
    footer = bytearray(original[footer_start:-10])
    for entry in order:
        offset, metadata_size, body_size = struct.unpack_from("<qi4xq", original, entry)
        struct.pack_into("<q", footer, entry - footer_start, len(relocated))
        relocated.extend(original[offset:offset + metadata_size + body_size])
    relocated.extend(struct.pack("<II", 0xFFFFFFFF, 0))
    relocated.extend(footer)
    relocated.extend(original[-10:])
    path = out / f"{name}.arrow"
    path.write_bytes(relocated)
    assert ipc.open_file(path).read_all().to_pydict() == expected

# Empty value batches exercise duplicate base-dictionary metadata without decoding any row indices.
empty = pa.record_batch([dictionary([], []), dictionary([], [])], names=["a", "b"])
with ipc.new_file(out / "duplicate_base.arrow", empty.schema) as writer:
    writer.write_batch(empty)
data = bytearray((out / "duplicate_base.arrow").read_bytes())
_, dictionaries, _ = file_blocks(data)
second_offset = struct.unpack_from("<q", data, dictionaries[1])[0]
message = indirect(data, second_offset + 8)
dictionary_batch = indirect(data, field(data, message, 2))
struct.pack_into("<q", data, field(data, dictionary_batch, 0), 0)
(out / "duplicate_base.arrow").write_bytes(data)
PYEOF

for CASE in interleaved records_first reversed_dictionaries; do
    cat <<SQL
SELECT '${CASE}';
SELECT a, b FROM file('${TMP_DIR}/${CASE}.arrow', 'Arrow', 'a String, b String');
SQL
done > "$TMP_DIR/queries.sql"

cat >> "$TMP_DIR/queries.sql" <<SQL
SELECT a FROM file('${TMP_DIR}/duplicate_base.arrow', 'Arrow', 'a String, b String'); -- { serverError INCORRECT_DATA }
SELECT 'Duplicate base dictionary rejected';
SQL

${CLICKHOUSE_LOCAL} --path "$TMP_DIR/local" --max_threads=1 \
    --multiquery --queries-file "$TMP_DIR/queries.sql"

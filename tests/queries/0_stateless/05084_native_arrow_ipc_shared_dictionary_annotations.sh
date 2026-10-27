#!/usr/bin/env bash
# Tags: no-fasttest
# This test requires PyArrow to write and validate shared dictionary files and streams.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PY'
from pathlib import Path
import json
import os
import shlex
import struct
import subprocess
import sys
import uuid

import pyarrow as pa
import pyarrow.ipc as ipc

out = Path(sys.argv[1])
local = shlex.split(os.environ["CLICKHOUSE_LOCAL"])
queries = []
checks = []


def indirect(data, position):
    return position + struct.unpack_from("<I", data, position)[0]


def field(data, table, index):
    vtable = table - struct.unpack_from("<i", data, table)[0]
    return table + struct.unpack_from("<H", data, vtable + 4 + 2 * index)[0]


def share_schema_dictionary(data, schema):
    fields = indirect(data, field(data, schema, 1))
    second = indirect(data, fields + 8)
    encoding = indirect(data, field(data, second, 4))
    struct.pack_into("<q", data, field(data, encoding, 0), 0)


def share_dictionary(data, fmt):
    # PyArrow assigns one dictionary id per field. Both fields use id zero after the schema rewrite,
    # and the unused second dictionary message is removed while footer block offsets remain accurate.
    data = bytearray(data)
    message_start = 8 if fmt == "Arrow" else 0
    message = indirect(data, message_start + 8)
    share_schema_dictionary(data, indirect(data, field(data, message, 2)))
    if fmt == "ArrowStream":
        messages = list(ipc.MessageReader.open_stream(pa.py_buffer(data)))
        kept_messages = (messages[0], messages[1], messages[3])
        stream = b"".join(message.serialize().to_pybytes() for message in kept_messages)
        return stream + struct.pack("<II", 0xFFFFFFFF, 0)

    footer_size = struct.unpack_from("<i", data, len(data) - 10)[0]
    footer = indirect(data, len(data) - 10 - footer_size)
    share_schema_dictionary(data, indirect(data, field(data, footer, 1)))
    dictionaries = indirect(data, field(data, footer, 2))
    second = dictionaries + 4 + 24
    offset, metadata_size, body_size = struct.unpack_from("<qi4xq", data, second)
    removed_size = metadata_size + body_size
    struct.pack_into("<I", data, dictionaries, 1)
    records = indirect(data, field(data, footer, 3))
    record_offset = struct.unpack_from("<q", data, records + 4)[0]
    struct.pack_into("<q", data, records + 4, record_offset - removed_size)
    del data[offset:offset + removed_size]
    return bytes(data)


cases = []
words = pa.array(["alpha", "beta"])
cases.append(("annotations", words, words, {"description": "left"}, {"description": "right"}))

# Element labels do not affect list values, including fixed-size and large-offset layouts.
for name, make_type, values in (
    ("list_names", pa.list_, [[10], [20, 30]]),
    ("large_list_names", pa.large_list, [[10], [20, 30]]),
    ("fixed_list_names", lambda child: pa.list_(child, 2), [[10, 20], [30, 40]]),
):
    left = pa.array(values, type=make_type(pa.field("first", pa.int32())))
    right = pa.array(values, type=make_type(pa.field("second", pa.int32())))
    cases.append((name, left, right, None, None))

# Struct element names are significant, while annotations on those elements are not.
values = [{"value": 10}, {"value": 20}]
left = pa.array(values, type=pa.struct([pa.field("value", pa.int32(), metadata={"note": "left"})]))
right = pa.array(values, type=pa.struct([pa.field("value", pa.int32(), metadata={"note": "right"})]))
cases.append(("struct_annotations", left, right, None, None))

# Map entries contain a struct whose value can itself contain list elements with different labels.
map_values = [[("x", [10])], [("y", [20, 30])]]
left = pa.array(map_values, type=pa.map_(pa.string(), pa.list_(pa.field("first", pa.int32()))))
right = pa.array(map_values, type=pa.map_(pa.string(), pa.list_(pa.field("second", pa.int32()))))
cases.append(("map_list_names", left, right, None, None))

# Union alternatives retain their names while unrelated field annotations can differ.
for mode in ("dense", "sparse"):
    ids = pa.array([0, 1], type=pa.int8())
    if mode == "dense":
        array = pa.UnionArray.from_dense(ids, pa.array([0, 0], type=pa.int32()),
                                        [pa.array([10], type=pa.int32()), pa.array(["x"])], ["number", "word"])
    else:
        array = pa.UnionArray.from_sparse(ids, [pa.array([10, 20], type=pa.int32()), pa.array(["x", "y"])],
                                         ["number", "word"])
    cases.append((f"{mode}_union_annotations", array, array, {"note": "left"}, {"note": "right"}))

# The same UUID interpretation remains compatible when unrelated annotations differ.
uuids = pa.array([bytes.fromhex("00112233445566778899aabbccddeeff"),
                  bytes.fromhex("112233445566778899aabbccddeeff00")], type=pa.binary(16))
cases.append(("uuid_annotations", uuids, uuids,
              {"PARQUET:logical_type": "UUID", "note": "left"},
              {"PARQUET:logical_type": "UUID", "note": "right"}))

for name, left, right, left_metadata, right_metadata in cases:
    columns = [pa.DictionaryArray.from_arrays(pa.array(indices, type=pa.int32()), values)
               for indices, values in (([0, 1], left), ([1, 0], right))]
    schema = pa.schema([pa.field("a", columns[0].type, metadata=left_metadata),
                        pa.field("b", columns[1].type, metadata=right_metadata)])
    batch = pa.record_batch(columns, schema=schema)
    expected = [[left[0].as_py(), right[1].as_py()], [left[1].as_py(), right[0].as_py()]]
    if name == "map_list_names":
        expected = [[dict(value) for value in row] for row in expected]
    if name == "uuid_annotations":
        expected = [[str(uuid.UUID(bytes=value)) for value in row] for row in expected]
    for fmt, writer_type, reader_type in (
        ("Arrow", ipc.new_file, ipc.open_file),
        ("ArrowStream", ipc.new_stream, ipc.open_stream),
    ):
        sink = pa.BufferOutputStream()
        with writer_type(sink, schema) as writer:
            writer.write_batch(batch)
        data = share_dictionary(sink.getvalue(), fmt)
        assert reader_type(pa.py_buffer(data)).read_all().to_pydict() == batch.to_pydict()
        path = out / f"{name}.{fmt}"
        path.write_bytes(data)
        queries.append(f"SELECT toJSONString(a), toJSONString(b) FROM file('{path}', '{fmt}') FORMAT TSVRaw;")
        checks.append((f"OK {name} {fmt}", expected))

result = subprocess.run(local + [
    "--path", f"{out}/local", "--max_threads=1", "--schema_inference_make_columns_nullable=0",
    "--multiquery", "--query", "\n".join(queries),
], text=True, capture_output=True)
assert result.returncode == 0 and not result.stderr, (result.returncode, result.stderr)
lines = result.stdout.splitlines()
position = 0
for name, expected in checks:
    actual = [[json.loads(value) for value in line.split("\t")] for line in lines[position:position + len(expected)]]
    assert actual == expected, (name, actual, expected)
    position += len(expected)
    print(name)
assert position == len(lines), lines[position:]
PY

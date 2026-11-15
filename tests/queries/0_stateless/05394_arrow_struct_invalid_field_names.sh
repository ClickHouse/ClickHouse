#!/usr/bin/env bash
# Tags: no-fasttest
# Arrow struct fields with empty or repeated names are read as an unnamed `Tuple`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILE="$CURDIR/data_arrow/struct_invalid_field_names.arrow"
STREAM="$CURDIR/data_arrow/struct_invalid_field_names.arrows"

$CLICKHOUSE_LOCAL -q "
    DESC file('$FILE', Arrow);
    SELECT * FROM file('$FILE', Arrow) ORDER BY id;
    DESC file('$STREAM', ArrowStream);
    SELECT * FROM file('$STREAM', ArrowStream) ORDER BY id;
    SELECT tupleElement(s_empty, 1), tupleElement(s_empty, 2), tupleElement(s_dup, 2) FROM file('$FILE', Arrow) ORDER BY id;
    SELECT s_empty, s_dup FROM file('$FILE', Arrow, 'id Int32, s_empty Tuple(Nullable(Int32), Nullable(String)), s_dup Tuple(Int32, String)') ORDER BY id;
    SELECT s_empty, s_dup FROM file('$FILE', Arrow, 'id Int32, s_empty Tuple(a Nullable(Int32), b Nullable(String)), s_dup Tuple(b Int32, a String)') ORDER BY id;
"

# The fields of such a struct get their requested types by position, also when the requested `Tuple` is named:
# a `fixed_size_binary(16)` field is read as `UInt128` only when it gets that type. Two equally named fields that
# share a dictionary keep their own requested types: the second one is range-checked as `Date32`.
DATA_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$DATA_DIR"
trap 'rm -rf "$DATA_DIR"' EXIT

python3 - "$DATA_DIR" <<'EOF'
import struct
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]


def indirect(data, position):
    return position + struct.unpack_from("<I", data, position)[0]


def field(data, table, index):
    vtable = table - struct.unpack_from("<i", data, table)[0]
    return table + struct.unpack_from("<H", data, vtable + 4 + 2 * index)[0]


binary = pa.array([(1).to_bytes(16, "little")], type=pa.binary(16))
s = pa.StructArray.from_arrays([binary], fields=[pa.field("", binary.type)])
with ipc.new_stream(f"{out}/binary.arrows", pa.schema([pa.field("s", s.type)])) as writer:
    writer.write_batch(pa.record_batch([s], names=["s"]))


def write_shared(path, day):
    dictionary = pa.array([day], type=pa.date32())
    children = [pa.DictionaryArray.from_arrays(pa.array([0], type=pa.int32()), dictionary) for _ in range(2)]
    s = pa.StructArray.from_arrays(children, fields=[pa.field("x", c.type) for c in children])
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, pa.schema([pa.field("s", s.type)])) as writer:
        writer.write_batch(pa.record_batch([s], names=["s"]))
    messages = list(ipc.MessageReader.open_stream(sink.getvalue()))
    # PyArrow gives each field its own dictionary: make the second field use the dictionary id of the first
    # one (Schema message -> fields[0] -> children[1] -> dictionary.id) and drop its dictionary batch.
    schema = bytearray(messages[0].serialize().to_pybytes())
    schema_table = indirect(schema, field(schema, indirect(schema, 8), 2))
    s_field = indirect(schema, indirect(schema, field(schema, schema_table, 1)) + 4)
    second = indirect(schema, indirect(schema, field(schema, s_field, 5)) + 8)
    struct.pack_into("<q", schema, field(schema, indirect(schema, field(schema, second, 4)), 0), 0)
    data = bytes(schema) + messages[1].serialize().to_pybytes() + messages[3].serialize().to_pybytes()
    data += struct.pack("<II", 0xFFFFFFFF, 0)
    table = ipc.open_stream(pa.py_buffer(data)).read_all()
    assert table.schema.field("s").type.field(1).type == table.schema.field("s").type.field(0).type
    with open(path, "wb") as f:
        f.write(data)


write_shared(f"{out}/shared_in_range.arrows", 19000)
write_shared(f"{out}/shared_out_of_range.arrows", 3000000)

# A named struct inside a struct whose only field has an empty name.
inner = pa.StructArray.from_arrays([pa.array([7], type=pa.int32())], fields=[pa.field("x", pa.int32())])
outer = pa.StructArray.from_arrays([inner], fields=[pa.field("", inner.type)])
nested = pa.schema([pa.field("s", outer.type)])
with ipc.new_file(f"{out}/nested.arrow", nested) as writer:
    writer.write_batch(pa.record_batch([outer], names=["s"]))
with ipc.new_stream(f"{out}/nested.arrows", nested) as writer:
    writer.write_batch(pa.record_batch([outer], names=["s"]))
EOF

$CLICKHOUSE_LOCAL -q "
    SELECT s FROM file('$DATA_DIR/binary.arrows', ArrowStream, 's Tuple(value UInt128)');
    SELECT s FROM file('$DATA_DIR/binary.arrows', ArrowStream, 's Tuple(UInt128)');
    SELECT s FROM file('$DATA_DIR/shared_in_range.arrows', ArrowStream, 's Tuple(Int64, Date32)');
    SELECT s FROM file('$DATA_DIR/shared_out_of_range.arrows', ArrowStream, 's Tuple(Int64, Int64)');
    SELECT s FROM file('$DATA_DIR/shared_out_of_range.arrows', ArrowStream, 's Tuple(Int64, Date32)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
"

# Case-insensitive matching still applies to named structs nested in such a struct, which is matched by position.
$CLICKHOUSE_LOCAL -q "
    SELECT s FROM file('$DATA_DIR/nested.arrow', Arrow, 's Tuple(payload Tuple(X Int32))') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
    SELECT s FROM file('$DATA_DIR/nested.arrows', ArrowStream, 's Tuple(payload Tuple(X Int32))') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
"

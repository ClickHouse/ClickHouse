#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow stream.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# An Arrow dictionary id may be shared by several fields, and a query may request different types for
# them. The values of a shared dictionary are decoded once for each field encoding it, as that field would
# decode them inline, and each field reads its own decoding: raw 16-byte binaries as `Int128` for one field
# and `UInt128` for the other, a `date32` dictionary holding an out-of-range day number as `Int32` and as
# `Int64`, while a `Date32` request for the same dictionary is rejected as it is for a flat column, and an
# in-range `date32` dictionary under `DateTime` targets that differ only in their timezone. A `LowCardinality`
# wrapper is a requested type of its own that yields the same values as the plain type, and a field is read
# alone so the other field's request plays no part.
#
# pyarrow gives every field its own dictionary id, so the stream is rewritten message by message: the second
# field of each pair is pointed at the first field's dictionary id in the schema, and its own dictionary
# batch is dropped. pyarrow reads the rewritten stream back as the independent check that it is a valid
# Arrow stream with shared dictionaries.

python3 - "$TMP_DIR" <<'PYEOF'
import struct, sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]

def i128(v):
    return v.to_bytes(16, "little", signed=True)

raw = pa.array([i128(12345), i128(-1), i128(67890)], type=pa.binary(16))
days = pa.array([3000000, 19000], type=pa.date32())
in_range_days = pa.array([19000, 18000], type=pa.date32())
batch = pa.record_batch(
    [pa.DictionaryArray.from_arrays(pa.array([0, 1, 2, 0], type=pa.int32()), raw),
     pa.DictionaryArray.from_arrays(pa.array([2, 2, 1, 0], type=pa.int32()), raw),
     pa.DictionaryArray.from_arrays(pa.array([0, 1, 0, 1], type=pa.int32()), days),
     pa.DictionaryArray.from_arrays(pa.array([1, 0, 1, 0], type=pa.int32()), days),
     pa.DictionaryArray.from_arrays(pa.array([0, 1, 0, 1], type=pa.int32()), in_range_days),
     pa.DictionaryArray.from_arrays(pa.array([1, 0, 1, 0], type=pa.int32()), in_range_days)],
    names=["a", "b", "d1", "d2", "t1", "t2"])

sink = pa.BufferOutputStream()
with ipc.new_stream(sink, batch.schema) as w:
    w.write_batch(batch)

# schema, then one dictionary batch per field in field order (ids 0..5), then the record batch
messages = []
reader = ipc.MessageReader.open_stream(sink.getvalue())
while True:
    try:
        message = reader.read_next_message()
    except StopIteration:
        break
    if message is None:
        break
    messages.append(message)
assert len(messages) == 8, len(messages)

# Point `b` at `a`'s dictionary (id 1 -> 0), `d2` at `d1`'s (id 3 -> 2) and `t2` at `t1`'s (id 5 -> 4). The
# ids are the only int64 values in the schema; pyarrow's read-back below verifies the result.
schema = bytearray(messages[0].metadata.to_pybytes())
for i in range(0, len(schema) - 7, 8):
    value = struct.unpack_from("<q", schema, i)[0]
    if value in (1, 3, 5):
        struct.pack_into("<q", schema, i, value - 1)

def framed(metadata, body):
    metadata = metadata + b"\x00" * (-len(metadata) % 8)
    return b"\xff\xff\xff\xff" + struct.pack("<i", len(metadata)) + metadata + body

def body_of(message):
    return message.body.to_pybytes() if message.body is not None else b""

stream = framed(bytes(schema), b"")
for message in [messages[1], messages[3], messages[5], messages[7]]:
    stream += framed(message.metadata.to_pybytes(), body_of(message))
stream += b"\xff\xff\xff\xff\x00\x00\x00\x00"
open(f"{out}/shared.arrows", "wb").write(stream)

table = ipc.open_stream(pa.py_buffer(stream)).read_all()
assert table.schema.field("a").type == table.schema.field("b").type
assert table.schema.field("d1").type == table.schema.field("d2").type
assert table.schema.field("t1").type == table.schema.field("t2").type
for name in table.column_names:
    chunk = table.column(name).chunk(0)
    values = chunk.dictionary
    values = values.cast(pa.int32()).to_pylist() if pa.types.is_date32(values.type) else [v.hex() for v in values.to_pylist()]
    print("pyarrow", name, chunk.indices.to_pylist(), values, sep="\t")
PYEOF

FILE="${TMP_DIR}/shared.arrows"
read_as() { ${CLICKHOUSE_LOCAL} --allow_suspicious_low_cardinality_types=1 --query "SELECT $1 FROM file('${FILE}', 'ArrowStream', '$2')" 2>&1; }

echo "--- a Int128, b UInt128 ---";                    read_as "a, b" "a Int128, b UInt128"
echo "--- a Int128, b LowCardinality(Int128) ---";     read_as "a, b" "a Int128, b LowCardinality(Int128)"
echo "--- b UInt128 alone ---";                        read_as "b" "b UInt128"
echo "--- a, b as String: the natural decoding ---";   read_as "hex(a), hex(b)" "a String, b String"
echo "--- d1 Int32, d2 Int64 ---";                     read_as "d1, d2" "d1 Int32, d2 Int64"
echo "--- d1 Int32, d2 LowCardinality(Int32) ---";     read_as "d1, d2" "d1 Int32, d2 LowCardinality(Int32)"
echo "--- d1 Int32, d2 Date32: the Date32 request still rejects the out-of-range day ---"
read_as "d1, d2" "d1 Int32, d2 Date32" | grep -o "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" | head -n 1
echo "--- t1 DateTime, t2 DateTime('UTC') ---";                read_as "toDate(t1), toDate(t2)" "t1 DateTime, t2 DateTime(''UTC'')"
echo "--- t1 DateTime64(3), t2 DateTime64(3, 'UTC') ---";      read_as "toDate(t1), toDate(t2)" "t1 DateTime64(3), t2 DateTime64(3, ''UTC'')"

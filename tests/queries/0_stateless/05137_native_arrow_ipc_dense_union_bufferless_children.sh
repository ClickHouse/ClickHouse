#!/usr/bin/env bash
# Tags: long, no-fasttest
# no-fasttest: needs the pyarrow and numpy Python modules to build the Arrow streams.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# A dense union child may declare more rows than the union selects, as a sliced child does. A buffer-less
# child — an empty struct, a fixed-size list of them, a struct of `null` fields — has no buffer whose size
# could bound that declared count, so the reader builds only the values the union selects and sizes nothing
# by the declaration. The large stream below forges the declared length of such a child to the largest
# count the message body could justify bit for bit; the read must produce what the original produces,
# within a memory limit the original read fits in. A child that declares nulls carries a validity bitmap
# whose size does bound the count, so its forged length is rejected. A sparse union's children must
# declare the union's own row count.

python3 - "$TMP_DIR" <<'PYEOF'
import struct
import sys
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]


def write(name, column):
    schema = pa.schema([pa.field("u", column.type)])
    with ipc.new_stream(f"{out}/{name}.arrows", schema) as writer:
        writer.write_batch(pa.record_batch([column], schema=schema))


def dense(children, names, type_ids, offsets):
    return pa.UnionArray.from_dense(
        pa.array(type_ids, type=pa.int8()), pa.array(offsets, type=pa.int32()), children, names)


# Buffer-less children with ordinary ClickHouse types, next to an int32 child and the `null` placeholder.
empty = pa.struct([])
pairs = pa.list_(empty, 2)
names = ["e", "i", "f", "z"]
type_ids = [0, 1, 2, 3, 0, 1, 2, 3]
write("dense_small", dense(
    [pa.array([{}] * 2, type=empty), pa.array([10, 20], type=pa.int32()),
     pa.array([[{}, {}]] * 2, type=pairs), pa.nulls(2)],
    names, type_ids, [0, 0, 0, 0, 1, 1, 1, 1]))
write("sparse_small", pa.UnionArray.from_sparse(
    pa.array(type_ids, type=pa.int8()),
    [pa.array([{}] * 8, type=empty), pa.array(range(1, 9), type=pa.int32()),
     pa.array([[{}, {}]] * 8, type=pairs), pa.nulls(8)],
    names))

# A large dense union alternating a struct of one `null` field with an int32. The struct child declares
# more rows than the union selects; its declared length is the only such number in the message metadata.
ROWS = 2_000_000
DECLARED = 1_200_000
struct_null = pa.struct([pa.field("n", pa.null())])
type_ids = np.tile(np.array([0, 1], dtype=np.int8), ROWS // 2)
offsets = np.repeat(np.arange(ROWS // 2, dtype=np.int32), 2)
int_child = pa.array(np.arange(ROWS // 2, dtype=np.int32))
struct_child = pa.Array.from_buffers(struct_null, DECLARED, [None], children=[pa.nulls(DECLARED)])
write("dense_large", dense([struct_child, int_child], ["s", "i"], type_ids, offsets))
# The same union whose struct child declares one null, hence carries a validity bitmap.
validity = np.full(DECLARED // 8, 0xFF, dtype=np.uint8)
validity[0] = 0xFE
struct_child_with_null = pa.Array.from_buffers(
    struct_null, DECLARED, [pa.py_buffer(validity.tobytes())], children=[pa.nulls(DECLARED)], null_count=1)
write("dense_large_nulls", dense([struct_child_with_null, int_child], ["s", "i"], type_ids, offsets))

# Forge every aligned int64 equal to the struct child's declared length — its own node and its `null`
# field's node — to the row count the record batch body could hold bit for bit.
for name in ("dense_large", "dense_large_nulls"):
    with open(f"{out}/{name}.arrows", "rb") as stream:
        reader = ipc.MessageReader.open_stream(stream)
        body_bytes = max(message.body.size for message in iter(reader.read_next_message, None) if message.body)
    data = bytearray(open(f"{out}/{name}.arrows", "rb").read())
    forged = 0
    for i in range(0, len(data) - 7, 8):
        if struct.unpack_from("<q", data, i)[0] == DECLARED:
            struct.pack_into("<q", data, i, body_bytes * 8)
            forged += 1
    assert forged >= 2, forged
    open(f"{out}/{name}_forged.arrows", "wb").write(data)
PYEOF

echo "--- dense union over buffer-less children ---"
${CLICKHOUSE_LOCAL} --query "SELECT u, toTypeName(u) FROM file('${TMP_DIR}/dense_small.arrows', 'ArrowStream')"
echo "--- sparse union over buffer-less children ---"
${CLICKHOUSE_LOCAL} --query "SELECT u, toTypeName(u) FROM file('${TMP_DIR}/sparse_small.arrows', 'ArrowStream')"

# The original read stays well under 60 MB. Sizing the struct child by its forged declaration would cost
# a visibility mask and a null map of one byte per declared row each, about 224 MB together.
read_large()
{
    ${CLICKHOUSE_LOCAL} --max_memory_usage=120000000 --query "
        SELECT count(), countIf(variantType(u) = 'Int32'), sum(variantElement(u, 'Int32'))
        FROM file('${TMP_DIR}/$1.arrows', 'ArrowStream', 'u Variant(Int32, Tuple(n Nullable(Nothing)))')" 2>&1
}

echo "--- large dense union, as written ---"
read_large dense_large
echo "--- large dense union, buffer-less child declaring the body's bit count ---"
read_large dense_large_forged
echo "--- the same child with a validity bitmap: rejected ---"
read_large dense_large_nulls_forged | grep -o "validity buffer is too small" | head -n 1

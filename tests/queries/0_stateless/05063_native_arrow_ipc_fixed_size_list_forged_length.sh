#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow stream.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# A fixed-size list multiplies its row count by `list_size`, so its child can be declared far larger than
# the message body could hold while every FieldNode length stays consistent. When that child is a struct
# whose first field is a `null` array and whose second is buffered, the `null` field would be sized by the
# declared count before the buffered field's buffer-size check could notice the batch is corrupt. The
# reader therefore rejects a child count beyond the body's physical bound before decoding the child.
#
# The stream below holds fixed_size_list<struct<n: null, v: int32>, 3> with 5 rows (15 elements). Its
# forged variant patches every int64 equal to 5 to 2^36 and every int64 equal to 15 to 3 * 2^36 — the batch
# length, the list's node and the struct's, `n`'s and `v`'s nodes — so the metadata is consistent and only
# the body gives the forgery away. The variant must be rejected without attempting the allocation.

python3 - "$TMP_DIR" <<'PYEOF'
import struct, sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
rows, list_size = 5, 3
child = pa.StructArray.from_arrays(
    [pa.nulls(rows * list_size, type=pa.null()), pa.array(range(rows * list_size), type=pa.int32())], names=["n", "v"])
arr = pa.FixedSizeListArray.from_arrays(child, list_size)
sink = pa.BufferOutputStream()
with ipc.new_stream(sink, pa.schema([pa.field("a", arr.type)])) as w:
    w.write_batch(pa.record_batch([arr], names=["a"]))
data = bytearray(sink.getvalue().to_pybytes())
open(f"{out}/fsl.arrows", "wb").write(data)

huge = 1 << 36
for i in range(0, len(data) - 7, 8):
    value = struct.unpack_from("<q", data, i)[0]
    if value == rows:
        struct.pack_into("<q", data, i, huge)
    elif value == rows * list_size:
        struct.pack_into("<q", data, i, huge * list_size)
open(f"{out}/fsl_forged.arrows", "wb").write(data)
PYEOF

STRUCTURE='a Array(Tuple(n Nullable(Nothing), v Int32))'
echo "--- the valid stream ---"
${CLICKHOUSE_LOCAL} --query "SELECT a FROM file('${TMP_DIR}/fsl.arrows', 'ArrowStream', '${STRUCTURE}')"

echo "--- the forged stream ---"
err=$(${CLICKHOUSE_LOCAL} --max_memory_usage=1G --query "SELECT a FROM file('${TMP_DIR}/fsl_forged.arrows', 'ArrowStream', '${STRUCTURE}') FORMAT Null" 2>&1)
case "$err" in
    *CANNOT_ALLOCATE_MEMORY*|*"bad_alloc"*|*MEMORY_LIMIT_EXCEEDED*) echo "the forged count drove an allocation" ;;
    *"fixed-size-list child declares "*"more than the "*"message body can hold"*) echo "rejected by the physical bound of the body" ;;
    *) echo "unexpected outcome: ${err}" ;;
esac

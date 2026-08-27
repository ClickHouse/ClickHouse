#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for accepting empty nested List/Map containers whose offsets
# buffer is 0 bytes.  Apache Arrow Java < 19.0.0 (bundled with Apache Spark)
# emits a 0-byte offsets buffer for any variable-width vector with valueCount==0.
# When every outer collection in a batch is empty, the inner List/Map vector has
# zero elements and a 0-byte offsets buffer.  Every other Arrow implementation
# (arrow-cpp, pyarrow, arrow-rs) accepts this; ClickHouse must too.  The fix:
# when the list chunk length is 0, no offsets are ever accessed, so 0 bytes are
# required; skip the offsets-buffer validation before the cast.
#
# Follow-up to the flat String/Binary fix; here the element/value type is
# irrelevant (Array(Array(Int32)) fails just like the String cases), so this
# exercises the List/Map offsets path, not the String/Binary path.
#
# Covers four shapes where every outer collection is empty:
#   (a) Array(Array(Int32))         -> inner list child length=0 (no String)
#   (b) Array(Array(String))        -> inner list child length=0
#   (c) Map(String, Array(String))  -> map value (list) child length=0
#   (d) Array(Map(String, Int32))   -> inner map child length=0

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PYEOF'
import io, sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]

def write_arrow(tbl):
    buf = io.BytesIO()
    with ipc.new_file(buf, tbl.schema) as w:
        w.write_table(tbl)
    return buf.getvalue()

def zero_byte_string_array():
    """String array with length=0 and a 0-byte offsets buffer."""
    return pa.Array.from_buffers(
        pa.string(), 0,
        [None, pa.py_buffer(b""), pa.py_buffer(b"")]
    )

def zero_byte_list_array(child):
    """List array with length=0 and a 0-byte offsets buffer, as Apache Arrow
    Java < 19.0.0 produces for an empty nested collection."""
    return pa.Array.from_buffers(
        pa.list_(child.type), 0,
        [None, pa.py_buffer(b"")], children=[child]
    )

def outer_empty(inner):
    """Two empty outer lists wrapping the given (empty) inner array."""
    return pa.ListArray.from_arrays(pa.array([0, 0, 0], type=pa.int32()), inner)

ids = pa.array([1, 2], type=pa.int32())

# (a) Array(Array(Int32)) with two empty outer arrays -> inner list child length=0.
inner_int = zero_byte_list_array(pa.array([], type=pa.int32()))
tbl_a = pa.table({"id": ids, "a": outer_empty(inner_int)})
open(f"{out}/nested_int.arrow", "wb").write(write_arrow(tbl_a))

# (b) Array(Array(String)) with two empty outer arrays -> inner list child length=0.
inner_str = zero_byte_list_array(zero_byte_string_array())
tbl_b = pa.table({"id": ids, "a": outer_empty(inner_str)})
open(f"{out}/nested_string.arrow", "wb").write(write_arrow(tbl_b))

# (c) Map(String, Array(String)) with two empty maps -> map value (list) child length=0.
map_col = pa.MapArray.from_arrays(
    pa.array([0, 0, 0], type=pa.int32()),
    zero_byte_string_array(),
    zero_byte_list_array(zero_byte_string_array()),
)
tbl_c = pa.table({"id": ids, "m": map_col})
open(f"{out}/nested_map_value.arrow", "wb").write(write_arrow(tbl_c))

# (d) Array(Map(String, Int32)) with two empty outer arrays -> inner map child length=0.
inner_map = pa.Array.from_buffers(
    pa.map_(pa.string(), pa.int32()), 0,
    [None, pa.py_buffer(b"")],
    children=[pa.StructArray.from_arrays(
        [zero_byte_string_array(), pa.array([], type=pa.int32())],
        names=["key", "value"])],
)
tbl_d = pa.table({"id": ids, "a": outer_empty(inner_map)})
open(f"{out}/nested_array_map.arrow", "wb").write(write_arrow(tbl_d))
PYEOF

# Both reading paths are affected: the native ClickHouse reader
# (input_format_arrow_use_native_reader=1, the default) and the Apache Arrow
# library reader (=0).  Exercise both.
for native in 1 0; do
    echo "native_reader=${native}"
    S="--input_format_arrow_use_native_reader=${native}"

    # (a) Array(Array(Int32)): two empty outer arrays.
    $CLICKHOUSE_LOCAL $S --query \
        "SELECT id, a FROM file('${TMP_DIR}/nested_int.arrow', Arrow) ORDER BY id"

    # (b) Array(Array(String)): two empty outer arrays.
    $CLICKHOUSE_LOCAL $S --query \
        "SELECT id, a FROM file('${TMP_DIR}/nested_string.arrow', Arrow) ORDER BY id"

    # (c) Map(String, Array(String)): two empty maps.
    $CLICKHOUSE_LOCAL $S --query \
        "SELECT id, m FROM file('${TMP_DIR}/nested_map_value.arrow', Arrow) ORDER BY id"

    # (d) Array(Map(String, Int32)): two empty outer arrays.
    $CLICKHOUSE_LOCAL $S --query \
        "SELECT id, a FROM file('${TMP_DIR}/nested_array_map.arrow', Arrow) ORDER BY id"
done

# DoS guard for the native reader: an empty nested container references zero child
# elements, so the child subtree must also be empty.  A buffer-less child type (e.g.
# a `Null` field) derives its size from the FieldNode length alone, so a forged-huge
# length must be rejected *before* decoding it (otherwise a tiny message drives an
# arbitrary column allocation).  Build an all-empty Array(Array(Nullable(Nothing)))
# and, for every aligned int64 in the message, forge it to 2^62: ClickHouse must
# never attempt a huge allocation - each variant is either rejected as INCORRECT_DATA
# or read successfully, and at least one variant exercises the empty-child guard.
python3 - "$TMP_DIR" <<'PYEOF'
import io, struct, sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
HUGE = 1 << 62

null_child = pa.array([], type=pa.null())
inner = pa.Array.from_buffers(pa.list_(pa.null()), 0, [None, pa.py_buffer(b"")], children=[null_child])
outer = pa.ListArray.from_arrays(pa.array([0, 0, 0], type=pa.int32()), inner)
buf = io.BytesIO()
with ipc.new_file(buf, pa.schema([("id", pa.int32()), ("a", outer.type)])) as w:
    w.write_batch(pa.record_batch([pa.array([1, 2], type=pa.int32()), outer], names=["id", "a"]))
data = bytearray(buf.getvalue())
open(f"{out}/null_leaf_valid.arrow", "wb").write(data)

for i in range(0, len(data) - 8, 8):
    if struct.unpack_from("<q", data, i)[0] == 0:
        patched = bytearray(data)
        patched[i:i + 8] = struct.pack("<q", HUGE)
        open(f"{out}/null_leaf_forged_{i}.arrow", "wb").write(patched)
PYEOF

STRUCT='id Int32, a Array(Array(Nullable(Nothing)))'

# The valid file reads as two empty arrays.
$CLICKHOUSE_LOCAL --input_format_arrow_use_native_reader=1 --query \
    "SELECT id, a FROM file('${TMP_DIR}/null_leaf_valid.arrow', Arrow, '${STRUCT}') ORDER BY id"

# Every forged variant must be handled without an allocation attempt (no OOM/crash).
oom=0
guard=0
for f in "${TMP_DIR}"/null_leaf_forged_*.arrow; do
    err=$($CLICKHOUSE_LOCAL --input_format_arrow_use_native_reader=1 \
        --max_memory_usage=1G \
        --query "SELECT id, a FROM file('${f}', Arrow, '${STRUCT}') FORMAT Null" 2>&1)
    case "$err" in
        *CANNOT_ALLOCATE_MEMORY*|*"bad_alloc"*|*"MEMORY_LIMIT_EXCEEDED"*) oom=$((oom + 1)) ;;
    esac
    case "$err" in
        *"references no elements but its child declares"*) guard=$((guard + 1)) ;;
    esac
done
echo "DoS guard: forged-length variants that drove an allocation: ${oom}"
[ "$guard" -ge 1 ] && echo "DoS guard: empty-child length check active" || echo "DoS guard: empty-child length check NOT triggered"

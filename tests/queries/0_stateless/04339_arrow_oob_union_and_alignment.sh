#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests for two more malformed-Arrow OOB/UB paths in the native Arrow IPC reader:
#   - a sparse union whose child FieldNode.length is shorter than the parent: every sparse-union child
#     must hold all parent rows, otherwise the per-row insertFrom / child null-map lookup reads past the
#     child column;
#   - a non-empty buffer at an unaligned (non-8-byte) offset, which the typed int32/int64 loads in the
#     string/list/map/dictionary/union decoders would dereference as undefined behavior.
# Each must be rejected as INCORRECT_DATA.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PYEOF'
import struct, io, sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]

def write_one(arr, name):
    sch = pa.schema([pa.field(name, arr.type)])
    buf = io.BytesIO()
    with ipc.new_file(buf, sch) as w:
        w.write_table(pa.Table.from_arrays([arr], schema=sch))
    return bytearray(buf.getvalue())

# Sparse union, 2 rows, an Int32 child and a Float64 child each of length 2; patch the Int32 child's
# FieldNode.length 2 -> 1. The FieldNode vector is [union(2,0), child0(2,0), child1(2,0)]; child0's
# length is the 3rd int64 (+16 from the vector start).
u = pa.UnionArray.from_sparse(
    pa.array([0, 1], type=pa.int8()),
    [pa.array([10, 20], type=pa.int32()), pa.array([1.5, 2.5], type=pa.float64())])
d = write_one(u, 'u')
idx = d.find(struct.pack('<6q', 2, 0, 2, 0, 2, 0))
assert idx >= 0, "sparse union FieldNode pattern not found"
d[idx + 16:idx + 24] = struct.pack('<q', 1)
open(f'{out}/union_short_child.arrow', 'wb').write(d)

# List(Int32) [[1,2,3]]: the child values buffer is {offset=8, length=12} (8-byte aligned, right after
# the 8-byte list offsets buffer). Patch its offset 8 -> 9 so the buffer is no longer 8-byte aligned but
# still inside the message body, exercising the alignment check rather than the bounds check.
d = write_one(pa.array([[1, 2, 3]], type=pa.list_(pa.int32())), 'x')
idx = d.find(struct.pack('<qq', 8, 12))
assert idx >= 0, "child values buffer {offset=8, length=12} not found"
d[idx:idx + 8] = struct.pack('<q', 9)
open(f'{out}/misaligned_offset.arrow', 'wb').write(d)
PYEOF

check_incorrect_data() {
    local label="$1"; shift
    local actual
    actual=$("$@" 2>&1)
    local exit_code=$?
    if echo "$actual" | grep -qF 'INCORRECT_DATA'; then
        echo 'INCORRECT_DATA'
    else
        local first_line
        first_line=$(echo "$actual" | head -1 | cut -c1-200)
        echo "FAIL [$label] expected INCORRECT_DATA (exit=${exit_code}); got: ${first_line:-<empty output>}"
    fi
}

check_incorrect_data union_short_child \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/union_short_child.arrow', Arrow)"

check_incorrect_data misaligned_offset \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/misaligned_offset.arrow', Arrow)"

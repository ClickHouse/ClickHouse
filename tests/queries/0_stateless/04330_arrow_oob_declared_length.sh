#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests for forged top-level Arrow lengths that made a reader reserve column memory
# before validating buffers (previously CANNOT_ALLOCATE_MEMORY instead of INCORRECT_DATA):
#   - fixed-width columns (Int32, Bool, Decimal) and an empty Struct whose RecordBatch/FieldNode
#     length is patched negative or to 2^62;
#   - a LargeList deriving a 2^62 flattened child length from 64-bit offsets over a one-element child;
#   - the JSON reader (Binary/LargeBinary with JSON logical type) reserving from the declared
#     length before validating the offsets buffer.
# Each must be rejected as INCORRECT_DATA before the reserve.

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
NEG = -1
HUGE = 1 << 62

def write_arrow(arr, name='arr'):
    tbl = pa.table({name: arr})
    buf = io.BytesIO()
    with ipc.new_file(buf, tbl.schema) as w:
        w.write_table(tbl)
    return bytearray(buf.getvalue())

# Fixed-width / empty-struct columns: patch every aligned int64 equal to the 5-row count
# (RecordBatch.length and FieldNode.length; the data values avoid 5) to a negative or huge value.
def patch_row_count(arr, new, name, count=5):
    data = bytearray(write_arrow(arr, name=name))
    pos = [i for i in range(0, len(data) - 7, 8) if struct.unpack_from('<q', data, i)[0] == count]
    assert len(pos) >= 2, f"expected >=2 aligned int64={count}, got {len(pos)}"
    for p in pos:
        data[p:p+8] = struct.pack('<q', new)
    return data

open(f'{out}/int32_neg_length.arrow', 'wb').write(patch_row_count(pa.array([11,22,33,44,66], type=pa.int32()), NEG, 'x'))
open(f'{out}/int32_huge_length.arrow', 'wb').write(patch_row_count(pa.array([11,22,33,44,66], type=pa.int32()), HUGE, 'x'))
open(f'{out}/bool_neg_length.arrow', 'wb').write(patch_row_count(pa.array([True,False,True,False,True], type=pa.bool_()), NEG, 'x'))
open(f'{out}/decimal_huge_length.arrow', 'wb').write(patch_row_count(pa.array([11,22,33,44,66], type=pa.decimal128(10,2)), HUGE, 'x'))
open(f'{out}/empty_struct_neg_length.arrow', 'wb').write(patch_row_count(pa.array([{} for _ in range(5)], type=pa.struct([])), NEG, 's'))

# LargeList deriving a huge flattened child length from 64-bit offsets while the child data buffer
# holds one Int32: offsets patched [0,1] -> [0,2^62] and child FieldNode.length -> 2^62.
ll = bytearray(write_arrow(pa.array([[123]], type=pa.large_list(pa.int32())), name='x'))
fn = ll.find(struct.pack('<4q', 1, 0, 1, 0))
assert fn >= 0, "LargeList FieldNode pattern not found"
ll[fn+16:fn+24] = struct.pack('<q', HUGE)
ob = ll.find(struct.pack('<2q', 0, 1))
assert ob >= 0, "LargeList offset buffer [0,1] not found"
ll[ob:ob+16] = struct.pack('<2q', 0, HUGE)
open(f'{out}/largelist_huge_child_length.arrow', 'wb').write(ll)

# JSON reader: Binary/LargeBinary with JSON logical type and a 1-row file whose RecordBatch and
# FieldNode length are forged to 2^30.
def write_json_binary(binary_type):
    field = pa.field('x', binary_type, metadata={b'PARQUET:logical_type': b'JSON'})
    sch = pa.schema([field])
    buf = io.BytesIO()
    with ipc.new_file(buf, sch) as w:
        w.write_table(pa.Table.from_arrays([pa.array([b'{"a":1}'], type=binary_type)], schema=sch))
    return bytearray(buf.getvalue())

def forge_one_row_to_huge(data, new_length):
    positions = [i for i in range(0, len(data) - 7, 8) if struct.unpack_from('<q', data, i)[0] == 1]
    for a in range(len(positions)):
        for b in range(a + 1, len(positions)):
            cand = bytearray(data)
            for p in (positions[a], positions[b]):
                cand[p:p+8] = struct.pack('<q', new_length)
            try:
                with ipc.open_file(pa.py_buffer(bytes(cand))) as reader:
                    t = reader.read_all()
                if t.num_rows == new_length and len(t.column('x').chunks[0]) == new_length:
                    return cand
            except Exception:
                pass
    raise RuntimeError("could not forge RecordBatch + FieldNode length")

open(f'{out}/binary_json_huge_length.arrow', 'wb').write(
    forge_one_row_to_huge(write_json_binary(pa.binary()), 1 << 30))
open(f'{out}/largebinary_json_huge_length.arrow', 'wb').write(
    forge_one_row_to_huge(write_json_binary(pa.large_binary()), 1 << 30))

# FixedSizeBinary(1) read with a UUID hint: the UUID reader reserves sizeof(UUID)=16 bytes per
# row, so it would allocate 16x the row count before the per-chunk byte_width check rejects the
# type. 1,000,000 one-byte rows (1 MB buffer) -> a 16 MB reserve; the width check must reject it
# as INCORRECT_DATA before reserving (the read below uses a memory limit below that reserve).
open(f'{out}/fixedbinary1_as_uuid.arrow', 'wb').write(
    write_arrow(pa.array([b'\x01'] * 1_000_000, type=pa.binary(1)), name='x'))
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

check_incorrect_data int32_neg_length \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/int32_neg_length.arrow', Arrow)"

check_incorrect_data int32_huge_length \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/int32_huge_length.arrow', Arrow)"

check_incorrect_data bool_neg_length \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/bool_neg_length.arrow', Arrow)"

check_incorrect_data decimal_huge_length \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/decimal_huge_length.arrow', Arrow)"

check_incorrect_data empty_struct_neg_length \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/empty_struct_neg_length.arrow', Arrow)"

check_incorrect_data largelist_huge_child_length \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/largelist_huge_child_length.arrow', Arrow)"

check_incorrect_data binary_json_huge_length \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/binary_json_huge_length.arrow', Arrow) FORMAT Null SETTINGS allow_experimental_json_type=1"

check_incorrect_data largebinary_json_huge_length \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/largebinary_json_huge_length.arrow', Arrow) FORMAT Null SETTINGS allow_experimental_json_type=1"

# The 8 MB memory limit is below the 16 MB UUID-column reserve but above the 1 MB input, so an
# implementation that reserves before checking byte_width fails with MEMORY_LIMIT_EXCEEDED; the
# fixed reader rejects the type with INCORRECT_DATA before reserving.
check_incorrect_data fixedbinary1_as_uuid \
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/fixedbinary1_as_uuid.arrow', Arrow, 'x UUID') FORMAT Null SETTINGS max_memory_usage=8000000"

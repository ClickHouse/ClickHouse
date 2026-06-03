#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests for heap out-of-bounds reads in Arrow IPC format readers
# (view-struct buffer and offsets-buffer readers).
#
#   17. readColumnWithViewData (StringViewArray) – view-struct buffer (16 bytes/row)
#   18. readColumnWithViewData (BinaryViewArray) – view-struct buffer
#   19. readColumnWithStringData  – offsets buffer over-read before per-row data check
#   20. readOffsetsFromArrowListColumn – List offsets Int32Array via Value()
#   21. getNestedArrowColumn (Map) – Map offsets array via ListArray::Flatten()
#   22. readColumnWithJSONData    – BinaryArray offsets buffer (same path as 19)
#   23. readColumnWithViewData    – view struct length field exceeds variadic data buffer
#   24. readColumnWithJSONData    – data buffer over-read (value_offset+length > data size)
#   25. readColumnWithBigNumberFromBinaryData – offsets buffer read before size check
#   26. readColumnWithGeoData     – offsets buffer read before per-row data-buffer check
#   27. readIPv6ColumnFromBinaryData – offsets buffer read before size check

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
LARGE_N = 16384

def write_arrow(arr, name='x'):
    tbl = pa.table({name: arr})
    buf = io.BytesIO()
    with ipc.new_file(buf, tbl.schema) as w:
        w.write_table(tbl)
    return bytearray(buf.getvalue())

def inflate_row_count(data, orig_n, large_n=LARGE_N):
    needle = struct.pack('<q', orig_n)
    patch  = struct.pack('<q', large_n)
    pos = 0
    while True:
        idx = data.find(needle, pos)
        if idx < 0:
            break
        if idx % 8 == 0:
            data[idx:idx+8] = patch
        pos = idx + 1
    return data

def shrink_first_buffer(data, full_len, new_len):
    target = struct.pack('<q', full_len)
    pos = 0
    while True:
        idx = data.find(target, pos)
        if idx < 0:
            raise AssertionError(f"buffer length {full_len} not found")
        if idx >= 8 and 0 <= struct.unpack_from('<q', data, idx-8)[0] <= len(data):
            data[idx:idx+8] = struct.pack('<q', new_len)
            return data
        pos = idx + 1

# 17. StringViewArray: the view-struct buffer (buffers[1], 16 bytes/element) must fit
#     all declared rows.  Strings > 12 bytes force non-inline view structs.
#     7 rows * 16 bytes = 112-byte buffer; shrink declared length to 16 (1 entry).
d = write_arrow(pa.array(['x'*13]*7, type=pa.string_view()))
open(f'{out}/strview.arrow', 'wb').write(shrink_first_buffer(d, 7*16, 16))

# 18. BinaryViewArray: same approach.
d = write_arrow(pa.array([b'x'*13]*7, type=pa.binary_view()))
open(f'{out}/binview.arrow', 'wb').write(shrink_first_buffer(d, 7*16, 16))

# 19. String (BinaryArray): value_offset(i) reads the offsets buffer (buffers[1])
#     before the per-row data check; checkBinaryOffsetsBuffer catches it first.
#     1 row → tiny offsets buffer, inflated to 16384 rows.
d = write_arrow(pa.array(['hi'], type=pa.string()))
open(f'{out}/string.arrow', 'wb').write(inflate_row_count(d, 1))

# 20. List(Int32): readOffsetsFromArrowListColumn calls Value(i) on the offsets
#     Int32Array; checkedCast validates the offsets buffer.
d = write_arrow(pa.array([[42]], type=pa.list_(pa.int32())))
open(f'{out}/list.arrow', 'wb').write(inflate_row_count(d, 1))

# 21. Map(Int32, Int32): MapArray subclasses ListArray; getNestedArrowColumn calls
#     Flatten() which reads the offsets buffer.
d = write_arrow(pa.array([[(1, 2)]], type=pa.map_(pa.int32(), pa.int32())))
open(f'{out}/map.arrow', 'wb').write(inflate_row_count(d, 1))

# 22. readColumnWithJSONData: same offsets-buffer vulnerability as case 19, but the
#     reader is invoked via a JSON type hint rather than plain String.
d = write_arrow(pa.array([b'{"a":1}'], type=pa.binary()))
open(f'{out}/json.arrow', 'wb').write(inflate_row_count(d, 1))

# 23. View data-buffer: checkedCastView validates the view-struct buffer but the
#     length field inside each view struct is attacker-controlled and used as the
#     memcpy size against the variadic data buffer.
#     96-char non-inline string → patch last int32 of its view struct to ~512 MB.
d = write_arrow(pa.array(['X'*96], type=pa.string_view()))
def patch_last_i32(data, pattern, newval):
    i = data.rfind(pattern)
    assert i >= 0
    data[i:i+4] = struct.pack('<i', newval)
    return data
# The view struct is {int32 size=96, uint8[4] prefix='XXXX', int32 buffer_index, int32 offset}.
# Patch size=96 (first i32 matching b'\x60\x00\x00\x00' followed by 'XXXX') to huge.
d = patch_last_i32(d, struct.pack('<i', 96) + b'XXXX', 0x20000000)
open(f'{out}/view_dlen.arrow', 'wb').write(d)

# 24. JSON data-buffer: checkBinaryOffsetsBuffer guards the offsets buffer but the
#     per-row value_offset+value_length is not checked against value_data()->size().
#     1 row of 96 'A' bytes: offsets = [0, 96]. Patch the last offset to ~512 MB.
d = write_arrow(pa.array([b'A'*96], type=pa.binary()))
d = patch_last_i32(d, struct.pack('<ii', 0, 96), 0x20000000)
open(f'{out}/json_dlen.arrow', 'wb').write(d)

# 25. readColumnWithBigNumberFromBinaryData: value_length(i) reads the offsets buffer
#     before any offsets-buffer check; checkBinaryOffsetsBuffer must fire first.
d = write_arrow(pa.array([b'\xcc'*16], type=pa.binary()))
open(f'{out}/bignum.arrow', 'wb').write(inflate_row_count(d, 1))

# 26. readColumnWithGeoData: value_offset(i) and value_length(i) read the offsets
#     buffer before the per-row data check; checkBinaryOffsetsBuffer must fire first.
d = write_arrow(pa.array([b'\x01\x01\x00\x00\x00' + b'\x00'*16], type=pa.binary()))
open(f'{out}/geo.arrow', 'wb').write(inflate_row_count(d, 1))

# 27. readIPv6ColumnFromBinaryData: same offsets-buffer gap as case 25 but for IPv6.
d = write_arrow(pa.array([b'\x00'*16], type=pa.binary()))
open(f'{out}/ipv6_binary.arrow', 'wb').write(inflate_row_count(d, 1))
PYEOF

check() {
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

check strview  $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/strview.arrow', Arrow)"
check binview  $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/binview.arrow', Arrow)"
# String: offsets buffer over-read before per-row data check
check string   $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/string.arrow', Arrow)"
# List: offsets Int32Array read via Value() in readOffsetsFromArrowListColumn
check list     $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/list.arrow', Arrow)"
# Map: MapArray subclasses ListArray; offsets read via Flatten() in getNestedArrowColumn
check map      $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/map.arrow', Arrow)"
# JSON: BinaryArray with JSON type hint routes through readColumnWithJSONData
check json     $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/json.arrow', Arrow, 'x JSON') FORMAT Null"
# View data-buffer: view struct length field inflated beyond the variadic data buffer
check view_dlen  $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/view_dlen.arrow', Arrow)"
# JSON data-buffer: data offset+length inflated beyond value_data() buffer
check json_dlen  $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/json_dlen.arrow', Arrow, 'x JSON') FORMAT Null"
# BigNum: offsets buffer read via value_length() before checkBinaryOffsetsBuffer
check bignum     $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/bignum.arrow', Arrow, 'x Int128')"
# Geo: offsets buffer read via value_offset()/value_length() before checkBinaryOffsetsBuffer
check geo        $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/geo.arrow', Arrow, 'x Point')" \
                   --input_format_parquet_allow_geoparquet_parser=1
# IPv6: offsets buffer read via value_length() before checkBinaryOffsetsBuffer
check ipv6_binary $CLICKHOUSE_LOCAL --query "SELECT x FROM file('${TMP_DIR}/ipv6_binary.arrow', Arrow, 'x IPv6')"

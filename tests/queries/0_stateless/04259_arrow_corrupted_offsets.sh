#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for heap-buffer-overflow in readColumnWithStringData when an
# Arrow file carries corrupted intermediate offsets that make individual
# value_length(i) much larger than the actual values buffer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrow"
DATA_FILE_STREAM="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_stream.arrow"
DATA_FILE_NULLABLE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nullable.arrow"
trap 'rm -f "$DATA_FILE" "$DATA_FILE_STREAM" "$DATA_FILE_NULLABLE"' EXIT

# Build a well-formed Arrow file with 4 binary rows of 10 bytes each,
# then corrupt offset[1] from 10 to 0x40000000 (1 GiB).
# A vulnerable build would attempt a 1 GiB memcpy into a ~40-byte heap buffer;
# the fixed build must reject the file with INCORRECT_DATA.
python3 - "$DATA_FILE" <<'EOF'
import struct
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

path = sys.argv[1]

arr = pa.array([b'A' * 10, b'B' * 10, b'C' * 10, b'D' * 10], type=pa.binary())
tbl = pa.Table.from_arrays([arr], names=['x'])
with pa.OSFile(path, 'wb') as f:
    w = ipc.new_file(f, tbl.schema)
    w.write_table(tbl)
    w.close()

# Patch offsets [0, 10, 20, 30, 40] -> [0, 0x40000000, 20, 30, 40].
# value_length(0) = offset[1] - offset[0] = 1 GiB; the last entry is unchanged
# so the pre-flight reserve stays small, triggering a write overflow on the old code.
data = bytearray(open(path, 'rb').read())
needle = struct.pack('<IIIII', 0, 10, 20, 30, 40)
idx = data.find(needle)
assert idx >= 0, "could not locate offsets array"
data[idx + 4 : idx + 8] = struct.pack('<I', 0x40000000)
open(path, 'wb').write(bytes(data))
EOF

$CLICKHOUSE_LOCAL --query "SELECT count(), max(length(x)) FROM file('${DATA_FILE}', 'Arrow')" 2>&1 \
    | grep -oF 'INCORRECT_DATA' || echo 'FAIL: expected INCORRECT_DATA'

# Same payload via ArrowStream format.
python3 - "$DATA_FILE_STREAM" <<'EOF'
import struct
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

path = sys.argv[1]

arr = pa.array([b'A' * 10, b'B' * 10, b'C' * 10, b'D' * 10], type=pa.binary())
tbl = pa.Table.from_arrays([arr], names=['x'])
with pa.OSFile(path, 'wb') as f:
    w = ipc.new_stream(f, tbl.schema)
    w.write_table(tbl)
    w.close()

data = bytearray(open(path, 'rb').read())
needle = struct.pack('<IIIII', 0, 10, 20, 30, 40)
idx = data.find(needle)
assert idx >= 0, "could not locate offsets array"
data[idx + 4 : idx + 8] = struct.pack('<I', 0x40000000)
open(path, 'wb').write(bytes(data))
EOF

$CLICKHOUSE_LOCAL --query "SELECT count(), max(length(x)) FROM file('${DATA_FILE_STREAM}', 'ArrowStream')" 2>&1 \
    | grep -oF 'INCORRECT_DATA' || echo 'FAIL: expected INCORRECT_DATA'

# Nullable column: corrupt the last offset on a non-null row so its value_length
# becomes 1 GiB. The pre-flight must validate non-null rows in the nullable path.
python3 - "$DATA_FILE_NULLABLE" <<'EOF'
import struct
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

path = sys.argv[1]

# 3 rows: non-null / null / non-null. Well-formed offsets: [0, 10, 10, 20].
arr = pa.array([b'A' * 10, None, b'C' * 10], type=pa.binary())
tbl = pa.Table.from_arrays([arr], names=['x'])
with pa.OSFile(path, 'wb') as f:
    w = ipc.new_file(f, tbl.schema)
    w.write_table(tbl)
    w.close()

# Corrupt offsets[3]: 20 -> 0x40000000.
# Row 2 (non-null): value_offset = offsets[2] = 10, value_length = 0x40000000 - 10 = 1 GiB.
# Pre-flight must reject this with INCORRECT_DATA.
data = bytearray(open(path, 'rb').read())
needle = struct.pack('<IIII', 0, 10, 10, 20)
idx = data.find(needle)
assert idx >= 0, "could not locate offsets array"
data[idx + 12 : idx + 16] = struct.pack('<I', 0x40000000)
open(path, 'wb').write(bytes(data))
EOF

$CLICKHOUSE_LOCAL --query "SELECT x FROM file('${DATA_FILE_NULLABLE}', 'Arrow')" 2>&1 \
    | grep -oF 'INCORRECT_DATA' || echo 'FAIL: expected INCORRECT_DATA'

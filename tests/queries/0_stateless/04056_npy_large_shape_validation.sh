#!/usr/bin/env bash

# Verify that NPY files with large shape dimensions that don't match
# the actual file size are rejected early.
# https://github.com/ClickHouse/ClickHouse/issues/99585

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/

# Test 1: Single dimension much larger than actual data
# shape=(2147483647,) but only 400 bytes of data (100 int32 elements)
python3 -c "
import struct, sys
magic = b'\x93NUMPY'
version = b'\x01\x00'
header = b\"{'descr': '<i4', 'fortran_order': False, 'shape': (2147483647,), }\"
pad_len = 64 - (len(magic) + len(version) + 2 + len(header)) % 64
if pad_len < 1: pad_len += 64
header += b' ' * (pad_len - 1) + b'\n'
header_len = struct.pack('<H', len(header))
sys.stdout.buffer.write(magic + version + header_len + header)
sys.stdout.buffer.write(struct.pack('<i', 42) * 100)
" > "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/large_1d.npy"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/large_1d.npy', 'Npy') LIMIT 1" 2>&1 | grep -m1 -o 'INCORRECT_DATA'

# Test 2: Multi-dimensional shape with large inner dimension
# shape=(1, 2147483647) but only 4000 bytes of data
python3 -c "
import struct, sys
magic = b'\x93NUMPY'
version = b'\x01\x00'
header = b\"{'descr': '<i4', 'fortran_order': False, 'shape': (1, 2147483647), }\"
pad_len = 64 - (len(magic) + len(version) + 2 + len(header)) % 64
if pad_len < 1: pad_len += 64
header += b' ' * (pad_len - 1) + b'\n'
header_len = struct.pack('<H', len(header))
sys.stdout.buffer.write(magic + version + header_len + header)
sys.stdout.buffer.write(struct.pack('<i', 42) * 1000)
" > "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/large_2d.npy"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/large_2d.npy', 'Npy') LIMIT 1" 2>&1 | grep -m1 -o 'INCORRECT_DATA'

# Test 3: Shape product that overflows size_t
# shape=(1, 2147483647, 2147483647) — product overflows
python3 -c "
import struct, sys
magic = b'\x93NUMPY'
version = b'\x01\x00'
header = b\"{'descr': '<i4', 'fortran_order': False, 'shape': (1, 2147483647, 2147483647), }\"
pad_len = 64 - (len(magic) + len(version) + 2 + len(header)) % 64
if pad_len < 1: pad_len += 64
header += b' ' * (pad_len - 1) + b'\n'
header_len = struct.pack('<H', len(header))
sys.stdout.buffer.write(magic + version + header_len + header)
sys.stdout.buffer.write(struct.pack('<i', 42) * 100)
" > "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/overflow_3d.npy"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/overflow_3d.npy', 'Npy') LIMIT 1" 2>&1 | grep -m1 -o 'INCORRECT_DATA'

# Test 4: Empty shape — must not crash (OOB on empty vector)
python3 -c "
import struct, sys
magic = b'\x93NUMPY'
version = b'\x01\x00'
header = b\"{'descr': '<i4', 'fortran_order': False, 'shape': (), }\"
pad_len = 64 - (len(magic) + len(version) + 2 + len(header)) % 64
if pad_len < 1: pad_len += 64
header += b' ' * (pad_len - 1) + b'\n'
header_len = struct.pack('<H', len(header))
sys.stdout.buffer.write(magic + version + header_len + header)
" > "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/empty_shape.npy"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/empty_shape.npy', 'Npy') LIMIT 1" 2>&1 | grep -m1 -o 'INCORRECT_DATA'

# Test 5: Unicode type with overflowing element size (size * 4 wraps to 0)
# <U4611686018427387904 → getSize() would return 0 without the overflow check
python3 -c "
import struct, sys
magic = b'\x93NUMPY'
version = b'\x01\x00'
header = b\"{'descr': '<U4611686018427387904', 'fortran_order': False, 'shape': (1,), }\"
pad_len = 64 - (len(magic) + len(version) + 2 + len(header)) % 64
if pad_len < 1: pad_len += 64
header += b' ' * (pad_len - 1) + b'\n'
header_len = struct.pack('<H', len(header))
sys.stdout.buffer.write(magic + version + header_len + header)
" > "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/unicode_overflow.npy"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/unicode_overflow.npy', 'Npy') LIMIT 1" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'

# Test 6: Huge 1D string element — per-row limit must catch this even for 1D
# |S8589934592 = 8 GiB element, shape=(1,)
python3 -c "
import struct, sys
magic = b'\x93NUMPY'
version = b'\x01\x00'
header = b\"{'descr': '|S8589934592', 'fortran_order': False, 'shape': (1,), }\"
pad_len = 64 - (len(magic) + len(version) + 2 + len(header)) % 64
if pad_len < 1: pad_len += 64
header += b' ' * (pad_len - 1) + b'\n'
header_len = struct.pack('<H', len(header))
sys.stdout.buffer.write(magic + version + header_len + header)
" > "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/huge_string.npy"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/huge_string.npy', 'Npy') LIMIT 1" 2>&1 | grep -m1 -o 'INCORRECT_DATA'

# Test 7: Offset memory amplification — small data but huge offset overhead
# shape=(1, 2147483648, 1) with <i1: 2B elements * (1 byte data + 16 bytes offsets) >> 2 GiB
python3 -c "
import struct, sys
magic = b'\x93NUMPY'
version = b'\x01\x00'
header = b\"{'descr': '|i1', 'fortran_order': False, 'shape': (1, 2147483648, 1), }\"
pad_len = 64 - (len(magic) + len(version) + 2 + len(header)) % 64
if pad_len < 1: pad_len += 64
header += b' ' * (pad_len - 1) + b'\n'
header_len = struct.pack('<H', len(header))
sys.stdout.buffer.write(magic + version + header_len + header)
" > "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/offset_amplification.npy"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/offset_amplification.npy', 'Npy') LIMIT 1" 2>&1 | grep -m1 -o 'INCORRECT_DATA'

# Test 8: Zero-size string dtype |S0 — valid NumPy type, must not be rejected
# Verify both count() (uses countRows fast path) and SELECT * (uses readRow)
python3 -c "
import numpy as np
np.save('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/s0.npy', np.ndarray(shape=(3,), dtype='|S0'))
"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/s0.npy', 'Npy')"
${CLICKHOUSE_CLIENT} --query "SELECT length(array) FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/s0.npy', 'Npy')"

# Test 9: Zero-size Unicode dtype <U0
python3 -c "
import numpy as np
np.save('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/u0.npy', np.ndarray(shape=(2,), dtype='<U0'))
"

${CLICKHOUSE_CLIENT} --query "SELECT length(array) FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/u0.npy', 'Npy')"

# Test 10: Multi-dimensional zero-size string: shape=(2, 3) with |S0
python3 -c "
import numpy as np
np.save('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/s0_2d.npy', np.ndarray(shape=(2, 3), dtype='|S0'))
"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/s0_2d.npy', 'Npy')"

# Test 11: Valid file still works
python3 -c "
import numpy as np
np.save('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/valid_shape.npy', np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int32))
"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/valid_shape.npy', 'Npy')"

rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}

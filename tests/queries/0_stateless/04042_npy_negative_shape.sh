#!/usr/bin/env bash

# Verify that NPY files with negative shape dimensions are rejected.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Generate NPY file with negative shape dimension: shape=(-1,)
python3 -c "
import struct, sys
magic = b'\x93NUMPY'
version = b'\x01\x00'
header = b\"{'descr': '<i4', 'fortran_order': False, 'shape': (-1,), }\"
pad_len = 64 - (len(magic) + len(version) + 2 + len(header)) % 64
if pad_len < 1: pad_len += 64
header += b' ' * (pad_len - 1) + b'\n'
header_len = struct.pack('<H', len(header))
sys.stdout.buffer.write(magic + version + header_len + header)
sys.stdout.buffer.write(struct.pack('<i', 42) * 4)
" > "${CLICKHOUSE_TMP}/negative_shape.npy"

# Should fail with INCORRECT_DATA, not hang
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${CLICKHOUSE_TMP}/negative_shape.npy', 'Npy')" 2>&1 | grep -o 'INCORRECT_DATA'

# Generate NPY file with negative shape in multi-dimensional: shape=(2, -3)
python3 -c "
import struct, sys
magic = b'\x93NUMPY'
version = b'\x01\x00'
header = b\"{'descr': '<i4', 'fortran_order': False, 'shape': (2, -3), }\"
pad_len = 64 - (len(magic) + len(version) + 2 + len(header)) % 64
if pad_len < 1: pad_len += 64
header += b' ' * (pad_len - 1) + b'\n'
header_len = struct.pack('<H', len(header))
sys.stdout.buffer.write(magic + version + header_len + header)
sys.stdout.buffer.write(struct.pack('<i', 42) * 6)
" > "${CLICKHOUSE_TMP}/negative_shape_multi.npy"

${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${CLICKHOUSE_TMP}/negative_shape_multi.npy', 'Npy')" 2>&1 | grep -o 'INCORRECT_DATA'

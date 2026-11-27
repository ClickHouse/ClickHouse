#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NPY_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_zero_inner.npy"
trap 'rm -f "$NPY_FILE"' EXIT

python3 -c "
import struct, sys
magic = b'\x93NUMPY'
version = b'\x01\x00'
header = b\"{'descr': '<i4', 'fortran_order': False, 'shape': (3, 0), }\"
pad_len = 64 - (len(magic) + len(version) + 2 + len(header)) % 64
if pad_len < 1: pad_len += 64
header += b' ' * (pad_len - 1) + b'\n'
header_len = struct.pack('<H', len(header))
sys.stdout.buffer.write(magic + version + header_len + header)
" > "$NPY_FILE"

${CLICKHOUSE_LOCAL} --query "SELECT array FROM file('$NPY_FILE', Npy, 'array Array(Int32)') FORMAT TSVRaw"
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('$NPY_FILE', Npy, 'array Array(Int32)') SETTINGS optimize_count_from_files=0 FORMAT TSVRaw"
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('$NPY_FILE', Npy, 'array Array(Int32)') SETTINGS optimize_count_from_files=1 FORMAT TSVRaw"

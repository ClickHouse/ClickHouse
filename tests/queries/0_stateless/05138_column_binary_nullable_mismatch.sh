#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is gated behind `allow_experimental_column_binary_format`.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# A `ColumnBinary` descriptor and the declared type must agree about nullability, and a frame
# that disagrees is bad input, not an internal inconsistency: it arrives from a file, a network
# peer or a WASM guest, so it must be reported as a data error like every other malformed frame.
#
# The reader guarded only one direction - `COL_IS_NULLABLE` set against a non-`Nullable` declared
# type. In the other direction the declared `Nullable` was passed on as if it were the base type,
# and asking a `Nullable` for its fixed value width raises a logical error instead.
mkdir -p "${USER_FILES_PATH}"
FRAME_FILE="${USER_FILES_PATH}/05138_frame_${CLICKHOUSE_DATABASE}.bin"

# Frame: 16-byte header (magic `CBIN`, version 1, 2 reserved, num_rows=2, num_cols=1), one
# 40-byte descriptor (type COL_FIXED64, no null map, no offsets, data at 56, 16 bytes), then two
# `UInt64` values. The descriptor does not set `COL_IS_NULLABLE`.
python3 -c "
import struct, sys
hdr = struct.pack('<IHHII', 0x4E494243, 1, 0, 2, 1)
# ColDescriptor: type, null_offset, offsets_offset, data_offset, data_size
desc = struct.pack('<QQQQQ', 4, 0, 0, 56, 16)
sys.stdout.buffer.write(hdr + desc + struct.pack('<QQ', 11, 22))
" > "${FRAME_FILE}"

# Read as the matching non-nullable type: the frame is well-formed and decodes.
${CLICKHOUSE_CLIENT} --query \
    "SELECT n FROM file('${FRAME_FILE}', ColumnBinary, 'n UInt64') ORDER BY n"

# Read as `Nullable(UInt64)`: the descriptor carries no null map, so the frame cannot supply the
# declared type. This must be rejected as bad data.
${CLICKHOUSE_CLIENT} --query \
    "SELECT n FROM file('${FRAME_FILE}', ColumnBinary, 'n Nullable(UInt64)')" 2>&1 \
    | grep -oE 'INCORRECT_DATA|LOGICAL_ERROR' | head -1

# The opposite mismatch was already rejected and must stay rejected: a descriptor that sets
# `COL_IS_NULLABLE` against a declared non-`Nullable` type.
python3 -c "
import struct, sys
hdr = struct.pack('<IHHII', 0x4E494243, 1, 0, 2, 1)
# COL_FIXED64 | COL_IS_NULLABLE (0x20), null map at 56, data at 58
desc = struct.pack('<QQQQQ', 4 | 0x20, 56, 0, 58, 16)
sys.stdout.buffer.write(hdr + desc + b'\x00\x00' + struct.pack('<QQ', 11, 22))
" > "${FRAME_FILE}.nullable"

${CLICKHOUSE_CLIENT} --query \
    "SELECT n FROM file('${FRAME_FILE}.nullable', ColumnBinary, 'n UInt64')" 2>&1 \
    | grep -oE 'INCORRECT_DATA|LOGICAL_ERROR' | head -1

rm -f "${FRAME_FILE}" "${FRAME_FILE}.nullable"

#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is gated behind `allow_experimental_column_binary_format`.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# A `ColumnBinary` frame declares its column data section through descriptor offsets, which are
# just bytes off the wire. The reader must not size its frame buffer from them in one step: a
# frame whose descriptor claims a huge data section but carries none must be rejected on the
# short read, having allocated only as much as the input actually held, rather than committing
# to the declared size first. Reading in bounded chunks is what makes that true without imposing any
# limit on how large an honest frame may be.
#
# Frame: 16-byte header (magic `CBIN`, version 1, 2 reserved, num_rows, num_cols=1) followed by
# one 40-byte descriptor (type COL_FIXED64, no null map, no offsets, data at 56) whose data_size
# claims 1 EiB. Nothing follows. The size is past what any allocator can satisfy, so a reader
# that sized its buffer from it in one step dies on the allocation, while one that reads in
# bounded chunks never allocates more than a chunk and fails on the short read of the first
# one. Only the latter reports `CANNOT_READ_ALL_DATA`.
mkdir -p "${USER_FILES_PATH}"
FRAME_FILE="${USER_FILES_PATH}/05136_lying_frame_${CLICKHOUSE_DATABASE}.bin"

python3 -c "
import struct, sys
num_rows = 1  # the declared data_size below is what matters, not this
hdr = struct.pack('<IHHII', 0x4E494243, 1, 0, num_rows, 1)
# ColDescriptor: type, null_offset, offsets_offset, data_offset, data_size
desc = struct.pack('<QQQQQ', 4, 0, 0, 56, 1 << 60)
sys.stdout.buffer.write(hdr + desc)
" > "${FRAME_FILE}"

${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM file('${FRAME_FILE}', ColumnBinary, 'n UInt64')" 2>&1 \
    | grep -oE 'CANNOT_READ_ALL_DATA' | head -1

# An honest frame of the same shape still round-trips, so the rejection above is about the
# mismatch between declaration and content, not about the frame's size.
${CLICKHOUSE_CLIENT} --query "SELECT number AS n FROM numbers(100000) FORMAT ColumnBinary" > "${FRAME_FILE}.good"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(n) FROM file('${FRAME_FILE}.good', ColumnBinary, 'n UInt64')"

rm -f "${FRAME_FILE}" "${FRAME_FILE}.good"

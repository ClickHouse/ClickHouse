#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"
# The test uses `LowCardinality(UInt64)` because a fixed-width dictionary keeps the frame layout simple.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_suspicious_low_cardinality_types 1"

# Unlike `null_offset`, `offsets_offset` has no "absent" meaning for `COL_LOWCARD`: the index
# array is mandatory. A frame setting it to 0 would otherwise point the index array at the
# frame header, so a 1-row column takes its dictionary index from the low byte of `num_rows` -
# metadata reparsed as payload instead of a rejected frame.
BAD_FILE="${CLICKHOUSE_TMP}/04926_no_index_array.bin"
python3 - "$BAD_FILE" << 'EOF'
import struct
import sys

COL_FIXED64, COL_LOWCARD = 4, 8

# 16 header + 40 descriptor = 56, then
#   56: lowcard block = uint32 dict_row_count + uint8 width + uint8[3] pad + ColDescriptor
#  104: dictionary values uint64[2]
frame = struct.pack("<IHHII", 0x4E494243, 1, 0, 1, 1)
frame += struct.pack("<QQQQQ", COL_LOWCARD, 0, 0, 56, 64)  # offsets_offset = 0
frame += struct.pack("<I", 2)
frame += struct.pack("<BBBB", 1, 0, 0, 0)
frame += struct.pack("<QQQQQ", COL_FIXED64, 0, 0, 104, 16)
frame += struct.pack("<QQ", 0, 42)
assert len(frame) == 120, len(frame)
with open(sys.argv[1], "wb") as f:
    f.write(frame)
EOF

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04926"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04926 (v LowCardinality(UInt64)) ENGINE = Memory"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04926 FROM INFILE '${BAD_FILE}' FORMAT ColumnBinary" 2>&1 \
    | grep -c "COL_LOWCARD descriptor has no index array" || echo "NOT REJECTED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_04926"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04926"
rm -f "${BAD_FILE}"

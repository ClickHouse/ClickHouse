#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"
# The test uses `LowCardinality(UInt64)` because a fixed-width dictionary keeps the frame layout simple.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_suspicious_low_cardinality_types 1"

# A `COL_LOWCARD` dictionary descriptor must not set `COL_IS_CONST`: the writer never emits it,
# and the `ColumnConst` it decodes to is not a valid `ColumnUnique` holder. `ColumnUnique`
# rejects a nullable holder but not a const one, and its constructor calls
# `reverse_index.setColumn(getRawColumnPtr())`, whose `assert_cast` is a plain `static_cast` in
# release builds - so the frame must be rejected before reaching that cast.
BAD_FILE="${CLICKHOUSE_TMP}/04927_const_dict.bin"
python3 - "$BAD_FILE" << 'EOF'
import struct
import sys

COL_FIXED64, COL_LOWCARD, COL_IS_CONST = 4, 8, 0x80

# 16 header + 40 descriptor = 56, then
#   56: index array uint8[1], padded to the 4-byte-aligned lowcard block
#   60: lowcard block = uint32 dict_row_count + uint8 width + uint8[3] pad + ColDescriptor
#  108: dictionary values uint64[2]
frame = struct.pack("<IHHII", 0x4E494243, 1, 0, 1, 1)
frame += struct.pack("<QQQQQ", COL_LOWCARD, 0, 56, 60, 64)
frame += bytes([0])
frame += bytes(3)
frame += struct.pack("<I", 2)
frame += struct.pack("<BBBB", 1, 0, 0, 0)
frame += struct.pack("<QQQQQ", COL_FIXED64 | COL_IS_CONST, 0, 0, 108, 16)
frame += struct.pack("<QQ", 0, 42)
assert len(frame) == 124, len(frame)
with open(sys.argv[1], "wb") as f:
    f.write(frame)
EOF

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04927"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04927 (v LowCardinality(UInt64)) ENGINE = Memory"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04927 FROM INFILE '${BAD_FILE}' FORMAT ColumnBinary" 2>&1 \
    | grep -c "COL_LOWCARD dictionary descriptor must not set COL_IS_CONST" || echo "NOT REJECTED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_04927"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04927"
rm -f "${BAD_FILE}"

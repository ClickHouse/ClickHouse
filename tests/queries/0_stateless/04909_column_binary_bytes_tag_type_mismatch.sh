#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# The `COL_BYTES` branch of `readColumnFromDesc` always builds a `ColumnString`, unlike the
# `COL_FIXED*` branches which build the declared type. The tag comes from an untrusted frame,
# so a frame declaring `COL_BYTES` for a `UInt64` column would otherwise hand a `ColumnString`
# to `insertRangeFrom`, whose `assert_cast` is a plain `static_cast` in release builds. The
# frame must be rejected at the format boundary instead.
BAD_FILE="${CLICKHOUSE_TMP}/04909_bytes_tag_mismatch.bin"
python3 - "$BAD_FILE" << 'EOF'
import struct
import sys

COL_BYTES = 0

# 16 header + 40 descriptor = 56, then
#   56: offsets uint64[1]
#   64: data bytes[8]
frame = struct.pack("<IHHII", 0x4E494243, 1, 0, 1, 1)
frame += struct.pack("<QQQQQ", COL_BYTES, 0, 56, 64, 8)
frame += struct.pack("<Q", 8)
frame += b"abcdefgh"
with open(sys.argv[1], "wb") as f:
    f.write(frame)
EOF

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04909"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04909 (v UInt64) ENGINE = Memory"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04909 FROM INFILE '${BAD_FILE}' FORMAT ColumnBinary" 2>&1 \
    | grep -c "COL_BYTES descriptor does not match declared type" || echo "NOT REJECTED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_04909"

# A well-formed `String` frame must still round-trip.
VALID_FILE="${CLICKHOUSE_TMP}/04909_valid.bin"
rm -f "${VALID_FILE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04909_str"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04909_str (v String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "SELECT toString(number) AS v FROM numbers(3) INTO OUTFILE '${VALID_FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04909_str FROM INFILE '${VALID_FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT v FROM t_04909_str ORDER BY v"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04909"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04909_str"
rm -f "${BAD_FILE}" "${VALID_FILE}"

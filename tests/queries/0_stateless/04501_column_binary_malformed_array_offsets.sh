#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# ColumnBinary/ColumnBinary's COL_COMPLEX Array decoder trusts the wire's guest-controlled
# offsets array. A non-monotonic offsets array (e.g. [0, 3, 1]) must be rejected instead of
# being passed through to ColumnArray, where a per-row size difference could underflow into
# a huge value and drive an out-of-bounds read/allocation.
MALFORMED_FILE="${CLICKHOUSE_TMP}/04501_malformed_array.bin"
VALID_FILE="${CLICKHOUSE_TMP}/04501_valid_array.bin"

python3 - "$MALFORMED_FILE" "$VALID_FILE" << 'EOF'
import struct
import sys

malformed_path, valid_path = sys.argv[1], sys.argv[2]
num_rows, num_cols, COL_COMPLEX = 2, 1, 5

def build_frame(outer_offs, nested_payload):
    outer_bytes = b"".join(struct.pack("<Q", x) for x in outer_offs)
    data = outer_bytes + nested_payload
    header = struct.pack("<IHHII", 0x4E494243, 1, 0, num_rows, num_cols)
    data_offset = len(header) + 40
    desc = struct.pack("<QQQQQ", COL_COMPLEX, 0, 0, data_offset, len(data))
    return header + desc + data

# Non-monotonic outer offsets: offs[1]=3 > offs[2]=1.
with open(malformed_path, "wb") as f:
    f.write(build_frame([0, 3, 1], bytes([42])))

# Valid, monotonic offsets: row 0 has 1 element, row 1 has 1 element.
with open(valid_path, "wb") as f:
    f.write(build_frame([0, 1, 2], bytes([10, 20])))
EOF

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04501;
CREATE TABLE t_04501 (a Array(UInt8)) ENGINE = Memory;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04501 FROM INFILE '${MALFORMED_FILE}' FORMAT ColumnBinary" 2>&1 \
    | grep -o "COL_COMPLEX Array offsets must be non-decreasing"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04501 FROM INFILE '${VALID_FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_04501 ORDER BY a"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04501"
rm -f "${MALFORMED_FILE}" "${VALID_FILE}"

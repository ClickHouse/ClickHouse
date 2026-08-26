#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# `COL_VARIANT` needs both a discriminator array (`null_offset`) and a row offset array
# (`offsets_offset`); neither is optional, so 0 is not an "absent" sentinel there. A frame
# omitting the discriminators would otherwise point them at the frame header, so a 1-row column
# takes its discriminator from the low byte of `num_rows` and is accepted with no discriminator
# bytes on the wire; omitting the row offsets is the same hole for an all-null frame.
DISC_FILE="${CLICKHOUSE_TMP}/04928_no_discriminators.bin"
OFFS_FILE="${CLICKHOUSE_TMP}/04928_no_row_offsets.bin"
python3 - "$DISC_FILE" "$OFFS_FILE" << 'EOF'
import struct
import sys

COL_FIXED64, COL_VARIANT = 4, 6


def desc(type_, null_offset, offsets_offset, data_offset, data_size):
    return struct.pack("<QQQQQ", type_, null_offset, offsets_offset, data_offset, data_size)


# 16 header + 40 descriptor = 56, then
#   56: discriminators uint8[1]
#   57: row offsets uint32[1]
#   64: variant block = uint32 K + K x { uint8 global_d, uint8[3] pad, ColDescriptor }
#  112: the single alternative's payload uint64[1]
inner = desc(COL_FIXED64, 1, 0, 112, 8)  # null_offset repurposed as the sub-row count
block = struct.pack("<I", 1) + struct.pack("<BBBB", 0, 0, 0, 0) + inner + struct.pack("<Q", 42)


def build(path, null_offset, offsets_offset):
    frame = struct.pack("<IHHII", 0x4E494243, 1, 0, 1, 1)
    frame += desc(COL_VARIANT, null_offset, offsets_offset, 64, 56)
    frame += bytes([0])                # discriminator: alternative 0
    frame += struct.pack("<I", 0)      # row offset within the sub-column
    frame += bytes(3)                  # pad up to the variant block
    frame += block
    assert len(frame) == 120, len(frame)
    with open(path, "wb") as f:
        f.write(frame)


build(sys.argv[1], 0, 57)
build(sys.argv[2], 56, 0)
EOF

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04928"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04928 (v Variant(UInt64)) ENGINE = Memory"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04928 FROM INFILE '${DISC_FILE}' FORMAT ColumnBinary" 2>&1 \
    | grep -c "COL_VARIANT descriptor has no discriminators" || echo "NOT REJECTED"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04928 FROM INFILE '${OFFS_FILE}' FORMAT ColumnBinary" 2>&1 \
    | grep -c "COL_VARIANT descriptor has no row offsets" || echo "NOT REJECTED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_04928"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04928"
rm -f "${DISC_FILE}" "${OFFS_FILE}"

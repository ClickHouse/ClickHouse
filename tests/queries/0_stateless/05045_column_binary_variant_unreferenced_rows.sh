#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# `COL_VARIANT` checks that every discriminator/offset pair points at an existing sub-column
# row, but the converse must hold too: every decoded sub-column row has to be referenced by
# some row. A frame whose only row is null while the header still declares a one-row
# alternative would otherwise reach `ColumnVariant::validateState`, which reports the mismatch
# as a logical error instead of as the malformed input it is.
BAD_FILE="${CLICKHOUSE_TMP}/05045_unreferenced_variant_rows.bin"
GOOD_FILE="${CLICKHOUSE_TMP}/05045_referenced_variant_rows.bin"
python3 - "$BAD_FILE" "$GOOD_FILE" << 'EOF'
import struct
import sys

COL_FIXED64, COL_VARIANT, NULL_DISCRIMINATOR = 4, 6, 255


def desc(type_, null_offset, offsets_offset, data_offset, data_size):
    return struct.pack("<QQQQQ", type_, null_offset, offsets_offset, data_offset, data_size)


# 16 header + 40 descriptor = 56, then
#   56: discriminators uint8[1]
#   57: row offsets uint32[1]
#   64: variant block = uint32 K + K x { uint8 global_d, uint8[3] pad, ColDescriptor }
#  112: the single alternative's payload uint64[1]
def build(path, discriminator):
    inner = desc(COL_FIXED64, 1, 0, 112, 8)  # null_offset repurposed as the sub-row count
    block = struct.pack("<I", 1) + struct.pack("<BBBB", 0, 0, 0, 0) + inner + struct.pack("<Q", 42)
    frame = struct.pack("<IHHII", 0x4E494243, 1, 0, 1, 1)
    frame += desc(COL_VARIANT, 56, 57, 64, 56)
    frame += bytes([discriminator])
    frame += struct.pack("<I", 0)      # row offset within the sub-column
    frame += bytes(3)                  # pad up to the variant block
    frame += block
    assert len(frame) == 120, len(frame)
    with open(path, "wb") as f:
        f.write(frame)


# The alternative declares one row, but the only row is null and references nothing.
build(sys.argv[1], NULL_DISCRIMINATOR)
# The same frame with the row actually selecting alternative 0.
build(sys.argv[2], 0)
EOF

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05045"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05045 (v Variant(UInt64)) ENGINE = Memory"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_05045 FROM INFILE '${BAD_FILE}' FORMAT ColumnBinary" 2>&1 \
    | grep -c "COL_VARIANT alternative 0 declares 1 rows but only 0 are referenced" || echo "NOT REJECTED"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_05045 FROM INFILE '${GOOD_FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT v FROM t_05045"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05045"
rm -f "${BAD_FILE}" "${GOOD_FILE}"

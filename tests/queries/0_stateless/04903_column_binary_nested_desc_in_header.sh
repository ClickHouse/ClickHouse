#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"
# The test uses `LowCardinality(UInt64)` because a fixed-width dictionary keeps the frame layout simple.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_suspicious_low_cardinality_types 1"

# ColumnBinary/ColumnBinary embeds `ColDescriptor`s inside the `COL_VARIANT` and `COL_LOWCARD`
# blobs. Those nested descriptors are guest/network-controlled, and confining them to the
# parent blob alone is not enough: a malformed frame can point a nested descriptor back at the
# bytes of the very header that contains it, so the decoder reinterprets header bytes as
# payload and "succeeds" instead of rejecting the frame. Nested descriptors must start at or
# after the end of their embedded header.
VARIANT_FILE="${CLICKHOUSE_TMP}/04903_variant_self_ref.bin"
LOWCARD_FILE="${CLICKHOUSE_TMP}/04903_lowcard_self_ref.bin"

python3 - "$VARIANT_FILE" "$LOWCARD_FILE" << 'EOF'
import struct
import sys

variant_path, lowcard_path = sys.argv[1], sys.argv[2]

COL_FIXED64, COL_VARIANT, COL_LOWCARD = 4, 6, 8
DESC_BYTES = 40


def desc(type_, null_offset, offsets_offset, data_offset, data_size):
    return struct.pack("<QQQQQ", type_, null_offset, offsets_offset, data_offset, data_size)


# ── Variant(UInt64), 1 row ───────────────────────────────────────────────────
# Frame layout: header(16) + top descriptor(40) = 56, then
#   56: discriminators uint8[1]
#   57: row offsets uint32[1]
#   64: variant block = uint32 K + K x { uint8 global_d, uint8[3] pad, ColDescriptor }
#       payload for the sub-columns starts right after that header, at 64 + 4 + 44 = 112.
# The malformed frame points the single sub-variant's data at 64 — inside the variant
# header itself, which is still within the parent blob but before the payload region.
num_rows = 1
top_data_offset = 64
variant_header_bytes = 4 + (4 + DESC_BYTES)
variant_data_size = variant_header_bytes + 8

inner = desc(COL_FIXED64, 1, 0, top_data_offset, 8)  # null_offset repurposed as sub-row count
block = struct.pack("<I", 1) + struct.pack("<BBBB", 0, 0, 0, 0) + inner + struct.pack("<Q", 0)

frame = struct.pack("<IHHII", 0x4E494243, 1, 0, num_rows, 1)
frame += desc(COL_VARIANT, 56, 57, top_data_offset, variant_data_size)
frame += bytes([0])                    # discriminator: alternative 0
frame += struct.pack("<I", 0)          # row offset within the sub-column
frame += bytes(top_data_offset - 61)   # pad up to the variant block
frame += block
assert len(frame) == top_data_offset + variant_data_size, len(frame)
with open(variant_path, "wb") as f:
    f.write(frame)

# ── LowCardinality(UInt64), 1 row ────────────────────────────────────────────
# Frame layout: header(16) + top descriptor(40) = 56, then
#   56: index array uint8[1]
#   64: lowcard block = uint32 dict_row_count + uint8 width + uint8[3] pad + ColDescriptor
#       dictionary payload starts right after that header, at 64 + 48 = 112.
# The malformed frame points the dictionary at 64 — inside the lowcard header itself.
lc_header_bytes = 4 + 4 + DESC_BYTES
lc_data_offset = 64
lc_data_size = lc_header_bytes + 8

dict_desc = desc(COL_FIXED64, 0, 0, lc_data_offset, 8)
lc_block = struct.pack("<I", 1) + struct.pack("<BBBB", 1, 0, 0, 0) + dict_desc + struct.pack("<Q", 0)

frame = struct.pack("<IHHII", 0x4E494243, 1, 0, num_rows, 1)
frame += desc(COL_LOWCARD, 0, 56, lc_data_offset, lc_data_size)
frame += bytes([0])                  # index array: single index 0
frame += bytes(lc_data_offset - 57)  # pad up to the lowcard block
frame += lc_block
assert len(frame) == lc_data_offset + lc_data_size, len(frame)
with open(lowcard_path, "wb") as f:
    f.write(frame)
EOF

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04903_variant;
DROP TABLE IF EXISTS t_04903_lowcard;
CREATE TABLE t_04903_variant (v Variant(UInt64)) ENGINE = Memory;
CREATE TABLE t_04903_lowcard (v LowCardinality(UInt64)) ENGINE = Memory;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04903_variant FROM INFILE '${VARIANT_FILE}' FORMAT ColumnBinary" 2>&1 \
    | grep -c "COL_VARIANT sub-variant data range" || echo "NOT REJECTED"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04903_lowcard FROM INFILE '${LOWCARD_FILE}' FORMAT ColumnBinary" 2>&1 \
    | grep -c "COL_LOWCARD dictionary data range" || echo "NOT REJECTED"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_04903_variant"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_04903_lowcard"

# Well-formed frames produced by the writer itself must still round-trip.
VALID_VARIANT="${CLICKHOUSE_TMP}/04903_valid_variant.bin"
VALID_LOWCARD="${CLICKHOUSE_TMP}/04903_valid_lowcard.bin"
rm -f "${VALID_VARIANT}" "${VALID_LOWCARD}"

${CLICKHOUSE_CLIENT} --query "SELECT number::Variant(UInt64) AS v FROM numbers(3) INTO OUTFILE '${VALID_VARIANT}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT number::LowCardinality(UInt64) AS v FROM numbers(3) INTO OUTFILE '${VALID_LOWCARD}' FORMAT ColumnBinary"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04903_variant FROM INFILE '${VALID_VARIANT}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04903_lowcard FROM INFILE '${VALID_LOWCARD}' FORMAT ColumnBinary"

# Order by the typed subcolumn: `Variant` itself is rejected as an ORDER BY key.
${CLICKHOUSE_CLIENT} --query "SELECT v FROM t_04903_variant ORDER BY v.UInt64"
${CLICKHOUSE_CLIENT} --query "SELECT v FROM t_04903_lowcard ORDER BY v"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04903_variant;
DROP TABLE IF EXISTS t_04903_lowcard;
"
rm -f "${VARIANT_FILE}" "${LOWCARD_FILE}" "${VALID_VARIANT}" "${VALID_LOWCARD}"

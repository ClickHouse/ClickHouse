#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental until its `COLUMNAR_V1` frame header is versioned.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# `COL_LOWCARD` carries the dictionary of a `LowCardinality(Nullable(T))` column as an
# ordinary `Nullable` sub-column, but `ColumnUnique` does not store nullability that way:
# it keeps a non-nullable holder whose slot 0 is the reserved `NULL` sentinel and whose
# slot 1 is the nested default. The decoder must therefore check that the encoded
# dictionary really has that sentinel layout before rebuilding the unique column, or a
# malformed frame can move the `NULL` marker onto an ordinary value and have it silently
# reinterpreted as `NULL`.

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04904;
CREATE TABLE t_04904 (v LowCardinality(Nullable(String))) ENGINE = Memory;
"

# Well-formed frames produced by the writer itself must round-trip.
VALID_FILE="${CLICKHOUSE_TMP}/04904_valid.bin"
rm -f "${VALID_FILE}"
${CLICKHOUSE_CLIENT} --query "
SELECT if(number % 2 = 0, NULL, toString(number))::LowCardinality(Nullable(String)) AS v
FROM numbers(4) INTO OUTFILE '${VALID_FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04904 FROM INFILE '${VALID_FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT v IS NULL, v FROM t_04904 ORDER BY v NULLS FIRST"

# A frame whose dictionary null map marks a slot other than the reserved sentinel must be
# rejected rather than decoded with that value turned into NULL.
BAD_FILE="${CLICKHOUSE_TMP}/04904_bad_sentinel.bin"
python3 - "$BAD_FILE" << 'EOF'
import struct
import sys

COL_FIXED64, COL_LOWCARD, COL_IS_NULLABLE = 4, 8, 0x20
DESC_BYTES = 40


def desc(type_, null_offset, offsets_offset, data_offset, data_size):
    return struct.pack("<QQQQQ", type_, null_offset, offsets_offset, data_offset, data_size)


# 8 header + 40 top descriptor = 48, then
#   48: index array uint8[1]
#   52: lowcard block = uint32 dict_row_count + uint8 width + uint8[3] pad + ColDescriptor
#  100: dictionary null map uint8[3]
#  104: dictionary values uint64[3]
dict_rows = 3
lc_data_offset = 52
null_map_offset = 100
dict_data_offset = 104
lc_data_size = (dict_data_offset + dict_rows * 8) - lc_data_offset

frame = struct.pack("<II", 1, 1)
frame += desc(COL_LOWCARD, 0, 48, lc_data_offset, lc_data_size)
frame += bytes([0])                 # index array: the single row points at dictionary slot 0
frame += bytes(3)                   # pad to the 4-byte-aligned lowcard block
frame += struct.pack("<I", dict_rows)
frame += struct.pack("<BBBB", 1, 0, 0, 0)
frame += desc(COL_FIXED64 | COL_IS_NULLABLE, null_map_offset, 0, dict_data_offset, dict_rows * 8)
frame += bytes([0, 0, 1])           # NULL marked on slot 2 instead of the reserved slot 0
frame += struct.pack("<QQQ", 7, 0, 42)
assert len(frame) == dict_data_offset + dict_rows * 8, len(frame)
with open(sys.argv[1], "wb") as f:
    f.write(frame)
EOF

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t_04904_num;
CREATE TABLE t_04904_num (v LowCardinality(Nullable(UInt64))) ENGINE = Memory;
"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04904_num FROM INFILE '${BAD_FILE}' FORMAT ColumnBinary" 2>&1 \
    | grep -c "COL_LOWCARD nullable dictionary" || echo "NOT REJECTED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_04904_num"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04904;
DROP TABLE IF EXISTS t_04904_num;
"
rm -f "${VALID_FILE}" "${BAD_FILE}"

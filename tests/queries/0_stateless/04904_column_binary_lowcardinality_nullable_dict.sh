#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"
# `LowCardinality(Nullable(UInt64))` is a suspicious type; the fixed-width dictionary case
# below needs it both to create the table and to cast in the `SELECT` that writes the frame.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_suspicious_low_cardinality_types 1"

# `ColumnUnique` reserves the leading dictionary slots for its special values: slot 0 is the
# nested default for a plain dictionary, and for a nullable one slot 0 is the `NULL` sentinel
# with slot 1 the nested default. `COL_LOWCARD` writes no dictionary null map, so those
# reserved slots are the only thing conveying nullability. A frame whose dictionary is shorter
# than they require must be rejected as bad input rather than reaching `ColumnUnique` and
# surfacing as an internal `ILLEGAL_COLUMN`.

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04904;
DROP TABLE IF EXISTS t_04904_num;
CREATE TABLE t_04904 (v LowCardinality(Nullable(String))) ENGINE = Memory;
CREATE TABLE t_04904_num (v LowCardinality(Nullable(UInt64))) ENGINE = Memory;
"

# Well-formed frames produced by the writer itself must round-trip, nulls included.
VALID_FILE="${CLICKHOUSE_TMP}/04904_valid.bin"
rm -f "${VALID_FILE}"
${CLICKHOUSE_CLIENT} --query "
SELECT if(number % 2 = 0, NULL, toString(number))::LowCardinality(Nullable(String)) AS v
FROM numbers(4) INTO OUTFILE '${VALID_FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04904 FROM INFILE '${VALID_FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT v IS NULL, v FROM t_04904 ORDER BY v NULLS FIRST"

# A frame whose dictionary is too short for the reserved slots must be rejected.
BAD_FILE="${CLICKHOUSE_TMP}/04904_short_dict.bin"
python3 - "$BAD_FILE" << 'EOF'
import struct
import sys

COL_FIXED64, COL_LOWCARD = 4, 8
DESC_BYTES = 40


def desc(type_, null_offset, offsets_offset, data_offset, data_size):
    return struct.pack("<QQQQQ", type_, null_offset, offsets_offset, data_offset, data_size)


# 16 header + 40 top descriptor = 56, then
#   56: index array uint8[1]
#   60: lowcard block = uint32 dict_row_count + uint8 width + uint8[3] pad + ColDescriptor
#  108: dictionary values uint64[1]
# A LowCardinality(Nullable(UInt64)) dictionary needs at least 2 rows (NULL sentinel +
# nested default); this one claims exactly 1.
dict_rows = 1
lc_data_offset = 60
dict_data_offset = 108
lc_data_size = (dict_data_offset + dict_rows * 8) - lc_data_offset

frame = struct.pack("<IHHII", 0x4E494243, 1, 0, 1, 1)
frame += desc(COL_LOWCARD, 0, 56, lc_data_offset, lc_data_size)
frame += bytes([0])                 # index array: the single row points at dictionary slot 0
frame += bytes(3)                   # pad to the 4-byte-aligned lowcard block
frame += struct.pack("<I", dict_rows)
frame += struct.pack("<BBBB", 1, 0, 0, 0)
frame += desc(COL_FIXED64, 0, 0, dict_data_offset, dict_rows * 8)
frame += struct.pack("<Q", 42)
assert len(frame) == dict_data_offset + dict_rows * 8, len(frame)
with open(sys.argv[1], "wb") as f:
    f.write(frame)
EOF

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04904_num FROM INFILE '${BAD_FILE}' FORMAT ColumnBinary" 2>&1 \
    | grep -c "reserved leading slots require at least 2" || echo "NOT REJECTED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_04904_num"

# The same round-trip with a fixed-width dictionary: the `COL_FIXED*` branches build the
# declared type, so a dictionary handed the declared `Nullable(UInt64)` would build a
# `ColumnNullable`, which `ColumnUnique` rejects as a holder.
NUM_FILE="${CLICKHOUSE_TMP}/04904_valid_num.bin"
rm -f "${NUM_FILE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04904_num_rt"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04904_num_rt (v LowCardinality(Nullable(UInt64))) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "
SELECT if(number % 2 = 0, NULL, number)::LowCardinality(Nullable(UInt64)) AS v
FROM numbers(4) INTO OUTFILE '${NUM_FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04904_num_rt FROM INFILE '${NUM_FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT v IS NULL, v FROM t_04904_num_rt ORDER BY v NULLS FIRST"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04904;
DROP TABLE IF EXISTS t_04904_num;
DROP TABLE IF EXISTS t_04904_num_rt;
"
rm -f "${VALID_FILE}" "${BAD_FILE}" "${NUM_FILE}"

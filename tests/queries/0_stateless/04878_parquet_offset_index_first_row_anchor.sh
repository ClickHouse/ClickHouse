#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

GOOD_FLAT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_good_flat.parquet"
BAD_FLAT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_bad_flat.parquet"
GOOD_ARR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_good_arr.parquet"
BAD_ARR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_bad_arr.parquet"

${CLICKHOUSE_LOCAL} -q "
    INSERT INTO FUNCTION file('${GOOD_FLAT}', Parquet, 'id Int64, s String, p Int32')
    SELECT number, toString(number), number % 3 FROM numbers(20)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_write_page_index = 1"

${CLICKHOUSE_LOCAL} -q "
    INSERT INTO FUNCTION file('${GOOD_ARR}', Parquet, 'id Int64, a Array(Int64)')
    SELECT number, [number, number * 10] FROM numbers(20)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_write_page_index = 1"

# Set the first PageLocation.first_row_index of the last column chunk to a nonzero row.
# The location is found by structure, not by a fixed offset, because the byte layout depends
# on the writer. Thrift compact encoding of a PageLocation whose first_row_index is 0 is
# 0x16 <offset> 0x15 <size> 0x16 0x00 0x00.
patch_last_page_location() {
    python3 - "$1" "$2" <<'PY'
import struct, sys

src, dst = sys.argv[1], sys.argv[2]
d = bytearray(open(src, 'rb').read())
assert d[:4] == b'PAR1' and d[-4:] == b'PAR1', 'not a parquet file'
footer_start = len(d) - 8 - struct.unpack('<I', bytes(d[-8:-4]))[0]

def varint_len(i):
    n = 0
    while i + n < len(d) and (d[i + n] & 0x80):
        n += 1
    return n + 1

anchors = []
for i in range(footer_start):
    if d[i] != 0x16:
        continue
    j = i + 1 + varint_len(i + 1)
    if j >= footer_start or d[j] != 0x15:
        continue
    k = j + 1 + varint_len(j + 1)
    if k + 2 < footer_start and d[k] == 0x16 and d[k + 1] == 0x00 and d[k + 2] == 0x00:
        anchors.append(k + 1)

assert anchors, 'no offset index page location found'
d[anchors[-1]] = 0x08  # zigzag(4)
open(dst, 'wb').write(bytes(d))
PY
}

patch_last_page_location "${GOOD_FLAT}" "${BAD_FLAT}"
patch_last_page_location "${GOOD_ARR}" "${BAD_ARR}"

# Valid files still read correctly: the new check must not reject conforming offset indexes.
${CLICKHOUSE_LOCAL} -q "SELECT count(), sum(id) FROM file('${GOOD_FLAT}', Parquet)"
${CLICKHOUSE_LOCAL} -q "SELECT id, a FROM file('${GOOD_ARR}', Parquet) WHERE id = 10"

# A first page that does not start at row 0 is rejected as INCORRECT_DATA.
# Before the fix the flat read raised LOGICAL_ERROR in skipToRowOrNextPage, and the
# filtered read of the array column returned rows shifted by the corrupt anchor.
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${BAD_FLAT}', Parquet) FORMAT Null" 2>&1 | grep -c "INCORRECT_DATA"
${CLICKHOUSE_LOCAL} -q "SELECT id, a FROM file('${BAD_ARR}', Parquet) WHERE id = 10" 2>&1 | grep -c "INCORRECT_DATA"

rm -f "${GOOD_FLAT}" "${BAD_FLAT}" "${GOOD_ARR}" "${BAD_ARR}"

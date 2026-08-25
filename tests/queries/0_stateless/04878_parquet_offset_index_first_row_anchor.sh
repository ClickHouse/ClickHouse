#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Parquet format is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

GOOD_FLAT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_good_flat.parquet"
BAD_FLAT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_bad_flat.parquet"
GOOD_ARR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_good_arr.parquet"
BAD_ARR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_bad_arr.parquet"
BAD_TYPE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_bad_type.parquet"

${CLICKHOUSE_LOCAL} -q "
    INSERT INTO FUNCTION file('${GOOD_FLAT}', Parquet, 'id Int64, s String, p Int32')
    SELECT number, toString(number), number % 3 FROM numbers(20)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_write_page_index = 1"

${CLICKHOUSE_LOCAL} -q "
    INSERT INTO FUNCTION file('${GOOD_ARR}', Parquet, 'id Int64, a Array(Int64)')
    SELECT number, [number, number * 10] FROM numbers(20)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_write_page_index = 1"

# Corrupt one field of the offset index. Locations are found by structure, not by a fixed byte
# offset, because the byte layout depends on the writer. Thrift compact encoding of a
# PageLocation whose first_row_index is 0 is 0x16 <offset> 0x15 <size> 0x16 0x00 0x00, so each
# match also carries the byte offset of the page it describes.
#
#   anchor: set the last column chunk's first PageLocation.first_row_index to a nonzero row.
#   type:   leave every first_row_index at 0 and turn the first listed page into an INDEX_PAGE,
#           which the reader skips, so the page picked from the offset index for a requested row
#           turns out not to hold it.
patch_offset_index() {
    python3 - "$1" "$2" "$3" <<'PY'
import struct, sys

src, dst, mode = sys.argv[1], sys.argv[2], sys.argv[3]
d = bytearray(open(src, 'rb').read())
assert d[:4] == b'PAR1' and d[-4:] == b'PAR1', 'not a parquet file'
footer_start = len(d) - 8 - struct.unpack('<I', bytes(d[-8:-4]))[0]

def varint_len(i):
    n = 0
    while i + n < len(d) and (d[i + n] & 0x80):
        n += 1
    return n + 1

def read_zigzag(i):
    r = s = 0
    while True:
        b = d[i]
        i += 1
        r |= (b & 0x7F) << s
        if not (b & 0x80):
            return (r >> 1) ^ -(r & 1)
        s += 7

locations = []
for i in range(footer_start):
    if d[i] != 0x16:
        continue
    j = i + 1 + varint_len(i + 1)
    if j >= footer_start or d[j] != 0x15:
        continue
    k = j + 1 + varint_len(j + 1)
    if k + 2 < footer_start and d[k] == 0x16 and d[k + 1] == 0x00 and d[k + 2] == 0x00:
        locations.append((k + 1, read_zigzag(i + 1)))

assert locations, 'no offset index page location found'

if mode == 'anchor':
    d[locations[-1][0]] = 0x08  # zigzag(4)
else:
    page = locations[0][1]
    # Field 1 of PageHeader is the page type, an i32: 0x15 0x00 is zigzag(0) = DATA_PAGE.
    assert d[page] == 0x15 and d[page + 1] == 0x00, 'no data page header at offset %d' % page
    d[page + 1] = 0x02  # zigzag(1) = INDEX_PAGE

open(dst, 'wb').write(bytes(d))
PY
}

patch_offset_index "${GOOD_FLAT}" "${BAD_FLAT}" anchor
patch_offset_index "${GOOD_ARR}" "${BAD_ARR}" anchor
patch_offset_index "${GOOD_FLAT}" "${BAD_TYPE}" type

# Valid files still read correctly: the new check must not reject conforming offset indexes.
${CLICKHOUSE_LOCAL} -q "SELECT count(), sum(id) FROM file('${GOOD_FLAT}', Parquet)"
${CLICKHOUSE_LOCAL} -q "SELECT id, a FROM file('${GOOD_ARR}', Parquet) WHERE id = 10"

# A first page that does not start at row 0 is rejected as INCORRECT_DATA.
# Before the fix the flat read raised LOGICAL_ERROR in skipToRowOrNextPage, and the
# filtered read of the array column returned rows shifted by the corrupt anchor.
BAD_FLAT_ERR=$(${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${BAD_FLAT}', Parquet) FORMAT Null" 2>&1)
echo "${BAD_FLAT_ERR}" | grep -oF "Code: 117" | head -n 1
echo "${BAD_FLAT_ERR}" | grep -oF "Invalid offset index: first page starts at row" | head -n 1
BAD_ARR_ERR=$(${CLICKHOUSE_LOCAL} -q "SELECT id, a FROM file('${BAD_ARR}', Parquet) WHERE id = 10" 2>&1)
echo "${BAD_ARR_ERR}" | grep -oF "Code: 117" | head -n 1
echo "${BAD_ARR_ERR}" | grep -oF "Invalid offset index: first page starts at row" | head -n 1

# A page listed by the offset index that does not hold the requested row is rejected too.
BAD_TYPE_ERR=$(${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${BAD_TYPE}', Parquet) FORMAT Null" 2>&1)
echo "${BAD_TYPE_ERR}" | grep -oF "Code: 117" | head -n 1
echo "${BAD_TYPE_ERR}" | grep -oF "Page doesn't contain requested row" | head -n 1

rm -f "${GOOD_FLAT}" "${BAD_FLAT}" "${GOOD_ARR}" "${BAD_ARR}" "${BAD_TYPE}"

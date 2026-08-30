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
GOOD_INTERIOR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_good_interior.parquet"
BAD_INTERIOR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_bad_interior.parquet"

${CLICKHOUSE_LOCAL} -q "
    INSERT INTO FUNCTION file('${GOOD_FLAT}', Parquet, 'id Int64, s String, p Int32')
    SELECT number, toString(number), number % 3 FROM numbers(20)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_write_page_index = 1"

${CLICKHOUSE_LOCAL} -q "
    INSERT INTO FUNCTION file('${GOOD_ARR}', Parquet, 'id Int64, a Array(Int64)')
    SELECT number, [number, number * 10] FROM numbers(20)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_write_page_index = 1"

# The fixtures above have one page per column chunk. Small pages and small write batches give the
# array column several pages, which is what an interior page location needs.
${CLICKHOUSE_LOCAL} -q "
    INSERT INTO FUNCTION file('${GOOD_INTERIOR}', Parquet, 'id Int64, a Array(Int64)')
    SELECT number, [number, number * 10] FROM numbers(400)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_write_page_index = 1,
             output_format_parquet_data_page_size = 1024, output_format_parquet_batch_size = 64"

# Corrupt one field of the offset index. Locations are found by structure, not by a fixed byte
# offset, because the byte layout depends on the writer. Thrift compact encoding of a
# PageLocation whose first_row_index is 0 is 0x16 <offset> 0x15 <size> 0x16 0x00 0x00, so each
# match also carries the byte offset of the page it describes.
#
#   anchor:   set the last column chunk's first PageLocation.first_row_index to a nonzero row.
#   type:     leave every first_row_index at 0 and turn the first listed page into an INDEX_PAGE,
#             which the reader skips, so the page picked from the offset index for a requested row
#             turns out not to hold it.
#   interior: move an interior page's first_row_index forward, keeping the sequence strictly
#             increasing and in range so every check on the offset index alone still passes. Prints
#             the row it moved to.
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
        locations.append((k + 1, read_zigzag(i + 1), i))

assert locations, 'no offset index page location found'

def write_zigzag(v):
    assert v >= 0
    u, out = v << 1, bytearray()
    while True:
        out.append((u & 0x7F) | (0x80 if u > 0x7F else 0))
        u >>= 7
        if not u:
            return bytes(out)

def walk_locations(start):
    # A PageLocation list is written element after element, each of them
    # 0x16 <offset> 0x15 <compressed_page_size> 0x16 <first_row_index> 0x00, so the whole list can
    # be recovered from the first element. Returns [(offset, first_row_index_pos, first_row_index)].
    out = []
    i = start
    while i < footer_start and d[i] == 0x16:
        off_pos = i + 1
        j = off_pos + varint_len(off_pos)
        if j >= footer_start or d[j] != 0x15:
            break
        size_pos = j + 1
        k = size_pos + varint_len(size_pos)
        if k >= footer_start or d[k] != 0x16:
            break
        row_pos = k + 1
        e = row_pos + varint_len(row_pos)
        if e >= footer_start or d[e] != 0x00:
            break
        out.append((read_zigzag(off_pos), row_pos, read_zigzag(row_pos)))
        i = e + 1
    return out

if mode == 'anchor':
    d[locations[-1][0]] = 0x08  # zigzag(4)
elif mode == 'interior':
    # Keep only lists that describe a real multi-page data column: at least three pages, strictly
    # increasing first rows, and a DATA_PAGE header (0x15 0x00) where the first page starts. The
    # byte scan above can also match inside page data, and the flat column has too few pages.
    best = None
    for _, _, start in locations:
        locs = walk_locations(start)
        rows = [r for _, _, r in locs]
        if len(locs) < 3 or rows != sorted(set(rows)):
            continue
        off0 = locs[0][0]
        if off0 + 1 < footer_start and d[off0] == 0x15 and d[off0 + 1] == 0x00:
            best = locs
    assert best, 'no column chunk with at least three data pages found'
    # Move page 1 as far forward as its neighbour allows, so the span it declares no longer matches
    # the page's real row count. The new value must encode in as many bytes as the old one,
    # otherwise every later byte offset in the file shifts.
    old, nxt, row_pos = best[1][2], best[2][2], best[1][1]
    width = len(write_zigzag(old))
    new = min(nxt - 1, ((1 << (7 * width)) - 1) >> 1)
    assert old < new, 'no room to move page 1 (%d -> %d)' % (old, new)
    enc = write_zigzag(new)
    assert len(enc) == width, 'encoded width changed (%d -> %d)' % (width, len(enc))
    d[row_pos:row_pos + width] = enc
    print(new)
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
MOVED_ROW=$(patch_offset_index "${GOOD_INTERIOR}" "${BAD_INTERIOR}" interior)

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

# An interior page location that disagrees with the page's real row count is rejected. Array
# columns need their own check: their pages carry no row count in the header, so before the fix
# the read silently returned another page's values under the requested row numbers.
${CLICKHOUSE_LOCAL} -q "SELECT if(a[1] = id AND a[2] = id * 10, 'ok', 'wrong') FROM file('${GOOD_INTERIOR}', Parquet) WHERE id = ${MOVED_ROW}"
BAD_INTERIOR_ERR=$(${CLICKHOUSE_LOCAL} -q "SELECT id, a FROM file('${BAD_INTERIOR}', Parquet) WHERE id = ${MOVED_ROW}" 2>&1)
echo "${BAD_INTERIOR_ERR}" | grep -oF "Code: 117" | head -n 1
echo "${BAD_INTERIOR_ERR}" | grep -oF "doesn't match offset index: page has" | head -n 1

rm -f "${GOOD_FLAT}" "${BAD_FLAT}" "${GOOD_ARR}" "${BAD_ARR}" "${BAD_TYPE}" "${GOOD_INTERIOR}" "${BAD_INTERIOR}"

#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Parquet format is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

${CLICKHOUSE_LOCAL} --query "
    SELECT number AS a, toString(number) AS b FROM numbers(3)
    INTO OUTFILE '${WORK_DIR}/norm.parquet' FORMAT Parquet"

python3 - "$WORK_DIR" <<'PYEOF'
import struct, sys

work = sys.argv[1]

def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)

def read_varint(buf, pos):
    r = s = 0
    while True:
        b = buf[pos]
        pos += 1
        r |= (b & 0x7F) << s
        if not (b & 0x80):
            return r, pos
        s += 7

def footer_offset(buf):
    """Offset of the FileMetaData struct, right before the <len><PAR1> trailer."""
    assert bytes(buf[-4:]) == b"PAR1"
    return len(buf) - 8 - struct.unpack("<I", bytes(buf[-8:-4]))[0]

def skip_value(buf, pos, ctype):
    if ctype in (0x01, 0x02):  # bool, held in the field header
        return pos
    if ctype == 0x03:  # byte
        return pos + 1
    if ctype in (0x04, 0x05, 0x06):  # i16, i32, i64
        return read_varint(buf, pos)[1]
    if ctype == 0x07:  # double
        return pos + 8
    if ctype == 0x08:  # binary
        n, pos = read_varint(buf, pos)
        return pos + n
    if ctype in (0x09, 0x0A):  # list, set
        hdr, pos, n, etype = read_list_header(buf, pos)
        for _ in range(n):
            pos = skip_value(buf, pos, etype)
        return pos
    if ctype == 0x0B:  # map
        n, pos = read_varint(buf, pos)
        if n:
            kt, vt = (buf[pos] & 0xF0) >> 4, buf[pos] & 0x0F
            pos += 1
            for _ in range(n):
                pos = skip_value(buf, skip_value(buf, pos, kt), vt)
        return pos
    if ctype == 0x0C:  # struct
        return skip_struct(buf, pos)
    raise ValueError("ctype %d" % ctype)

def read_list_header(buf, pos):
    hdr = pos
    etype = buf[pos] & 0x0F
    n = (buf[pos] & 0xF0) >> 4
    pos += 1
    if n == 0xF:
        n, pos = read_varint(buf, pos)
    return hdr, pos, n, etype

def walk_struct(buf, pos, want=None, want_ctype=0x09):
    """Walk one struct; return the size header of field `want`, else the struct's end.

    For a list the header is the compact list header; for a binary field it is just the
    length varint. Both are `[start, end)` spans that can be rewritten in place.
    """
    last = 0
    while True:
        h = buf[pos]
        pos += 1
        if h == 0:  # STOP
            assert want is None, "field %d not found" % want
            return pos
        delta, ctype = (h & 0xF0) >> 4, h & 0x0F
        if delta == 0:
            zz, pos = read_varint(buf, pos)
            fid = (zz >> 1) ^ -(zz & 1)
        else:
            fid = last + delta
        last = fid
        if fid == want:
            assert ctype == want_ctype, "field %d has ctype %d" % (fid, ctype)
            if ctype == 0x08:  # binary
                true_len, end = read_varint(buf, pos)
                return pos, end, true_len
            hdr, end, _, etype = read_list_header(buf, pos)
            return hdr, end, etype
        pos = skip_value(buf, pos, ctype)

def skip_struct(buf, pos):
    return walk_struct(buf, pos)

def patch_size(name, hdr, end, new, buf):
    """Replace the size header at [hdr, end) and keep the footer length consistent."""
    buf[hdr:end] = new
    grew = len(new) - (end - hdr)
    buf[-8:-4] = struct.pack("<I", struct.unpack("<I", bytes(buf[-8:-4]))[0] + grew)
    open(f"{work}/{name}.parquet", "wb").write(buf)

def forge(name, count):
    """Rewrite `schema`'s element count to `count`, keeping the file otherwise intact."""
    buf = bytearray(open(f"{work}/norm.parquet", "rb").read())
    hdr, end, etype = walk_struct(buf, footer_offset(buf), want=2)
    patch_size(name, hdr, end, bytes([0xF0 | etype]) + varint(count), buf)

def forge_string(name, declared_len):
    """Rewrite `created_by`'s declared length, leaving its payload bytes untouched."""
    buf = bytearray(open(f"{work}/norm.parquet", "rb").read())
    hdr, end, _ = walk_struct(buf, footer_offset(buf), want=6, want_ctype=0x08)
    patch_size(name, hdr, end, varint(declared_len), buf)

# 5000 is small enough that the pre-fix allocation finishes in seconds instead of
# exhausting the runner, yet the error still differs with and without the fix.
forge("forged", 5000)
# 500 MB is declared but not present, so pre-fix this is one bounded realloc that then
# fails the short read; post-fix the limit rejects it before allocating.
forge_string("forged_str", 500000000)
PYEOF

echo "-- forged container length is rejected --"
${CLICKHOUSE_LOCAL} --query "DESCRIBE TABLE file('${WORK_DIR}/forged.parquet', Parquet)" 2>&1 \
    | grep -oF 'Exceeded size limit' | head -n 1

echo "-- forged string length is rejected --"
${CLICKHOUSE_LOCAL} --query "DESCRIBE TABLE file('${WORK_DIR}/forged_str.parquet', Parquet)" 2>&1 \
    | grep -oF 'Exceeded size limit' | head -n 1

echo "-- a valid file is still read --"
${CLICKHOUSE_LOCAL} --query "DESCRIBE TABLE file('${WORK_DIR}/norm.parquet', Parquet)"

echo "-- a wide but legal file is still read --"
${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM generateRandom('$(seq -s ', ' -f 'c%g UInt8' 2000)', 1, 1, 1) LIMIT 1
    INTO OUTFILE '${WORK_DIR}/wide.parquet' FORMAT Parquet"
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${WORK_DIR}/wide.parquet', Parquet)"

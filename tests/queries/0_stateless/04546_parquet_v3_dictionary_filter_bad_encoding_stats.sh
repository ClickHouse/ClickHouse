#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs pyarrow to craft the malformed Parquet file, and the Parquet format which is
# not built in fasttest.

# Regression test: `encoding_stats` is advisory metadata the dictionary-based row group filter uses
# to decide whether a column chunk is fully dictionary-encoded (see `columnChunkCanUseDictionaryFilter`
# in Reader.cpp). It must never turn a readable file into a hard failure: an out-of-range
# `page_type`/`encoding` value in `encoding_stats` (from a future writer or a buggy one) should just
# make that chunk ineligible for the optimization - the same as a missing `encoding_stats` - and fall
# back to a full scan, not raise `INCORRECT_DATA`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

python3 - "$WORK_DIR" <<'PYEOF'
import struct, sys
import pyarrow as pa
import pyarrow.parquet as pq

work = sys.argv[1]

# --- minimal Thrift compact protocol walker/patcher (records the byte offset of every scalar field) ---
CT_INT = {0x03, 0x04, 0x05, 0x06}  # byte, i16, i32, i64

def zigzag_decode(n): return (n >> 1) ^ -(n & 1)
def zigzag_encode(n, bits=64):
    m = (1 << bits) - 1; u = n & m
    return ((u << 1) ^ (-(u >> (bits - 1)) & m)) & m
def encode_varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F; n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n: break
    return bytes(out)

class Walker:
    def __init__(self, buf, pos): self.buf, self.pos, self.rec = buf, pos, []
    def byte(self): b = self.buf[self.pos]; self.pos += 1; return b
    def varint(self):
        r = s = 0
        while True:
            b = self.byte(); r |= (b & 0x7F) << s
            if not (b & 0x80): return r
            s += 7
    def zz(self): return zigzag_decode(self.varint())
    def struct(self, path):
        last = 0
        while True:
            h = self.byte()
            if h == 0: return
            d = (h & 0xF0) >> 4; ct = h & 0x0F
            fid = self.zz() if d == 0 else last + d
            last = fid
            self.field(ct, path + [fid])
    def field(self, ct, path):
        vs = self.pos; val = None
        if ct in (0x01, 0x02): val = (ct == 0x01)
        elif ct == 0x03: val = self.byte()
        elif ct in (0x04, 0x05, 0x06): val = self.zz()
        elif ct == 0x07: self.pos += 8
        elif ct == 0x08:
            slen = self.varint(); self.pos += slen
        elif ct in (0x09, 0x0A): self.lst(path)
        elif ct == 0x0C: self.struct(path)
        else: raise ValueError(f"ctype {ct}")
        if ct in CT_INT: self.rec.append((tuple(path), vs, self.pos, val))
    def lst(self, path):
        st = self.byte(); n = (st & 0xF0) >> 4; et = st & 0x0F
        if n == 0xF: n = self.varint()
        for i in range(n): self.field(et, path + [i])

def footer_offset(buf):
    """Offset of the FileMetaData footer struct, right after the PAR1/len/PAR1 trailer."""
    assert bytes(buf[-4:]) == b"PAR1"
    footer_len = struct.unpack("<I", bytes(buf[-8:-4]))[0]
    return len(buf) - 8 - footer_len

def patch(buf, off, field_path, signed_value):
    w = Walker(buf, off); w.struct([])
    for pi, vs, ve, val in w.rec:
        if pi == tuple(field_path):
            nb = encode_varint(zigzag_encode(signed_value))
            assert len(nb) == ve - vs, f"len change for {field_path}: {ve-vs}->{len(nb)} ({val}->{signed_value})"
            buf[vs:ve] = nb
            return buf
    raise KeyError(field_path)

# A single row group, fully dictionary-encoded low-cardinality string column: the 2 encoding_stats
# entries are [0]=dictionary page (DICTIONARY_PAGE/PLAIN) and [1]=data page (DATA_PAGE/RLE_DICTIONARY).
# ColumnMetaData.encoding_stats is field 13; PageEncodingStats.page_type/encoding are fields 1/2.
VALS = [("v%d" % (i % 4)) for i in range(200)]

def craft(name, field_path):
    base = f"{work}/{name}.parquet"
    pq.write_table(pa.table({"s": pa.array(VALS, type=pa.string())}), base,
                   data_page_version="2.0", use_dictionary=True, write_statistics=False)
    buf = bytearray(open(base, "rb").read())
    patch(buf, footer_offset(buf), field_path, -64)
    open(f"{work}/{name}_evil.parquet", "wb").write(buf)

# Out-of-range `page_type` on the data page's encoding_stats entry.
craft("badpagetype", (4, 0, 1, 0, 3, 13, 1, 1))
# Out-of-range `encoding` on the data page's encoding_stats entry.
craft("badencoding", (4, 0, 1, 0, 3, 13, 1, 2))
PYEOF

CH="${CLICKHOUSE_LOCAL} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0"

for f in badpagetype badencoding; do
    echo "-- $f --"
    for value in "v1" "no_such_value"; do
        out=$(${CH} --input_format_parquet_dictionary_filter_push_down=1048576 --query="
            select count() from file('${WORK_DIR}/${f}_evil.parquet', Parquet) where s = '${value}' FORMAT JSON" 2>&1)
        if echo "$out" | grep -q "INCORRECT_DATA"; then
            echo "${value}: UNEXPECTED exception: $out"
        else
            echo "${value}: $(echo "$out" | jq -c '{result: .data, rows_read: .statistics.rows_read}')"
        fi
    done
done

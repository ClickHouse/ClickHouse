#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs pyarrow to craft the malformed Parquet files.

# Regression test for a heap out-of-bounds read in the Parquet V3 native reader.
#
# DataPageV2 headers carry definition_levels_byte_length / repetition_levels_byte_length
# as signed i32. A negative value sign-extended to size_t, and the bounds check used
# wrapping addition (`def + rep > page.data.size()`), so a pair like (def=-1, rep=1)
# wrapped the sum to 0 and bypassed the check. A 2^64-sized std::span then reached the
# RLE level decoder, whose raw memcpy read far past the page buffer (heap disclosure).
#
# The reader must reject every crafted file with a clean INCORRECT_DATA exception
# instead of reading out of bounds. On an unpatched ASan/UBSan build each "wrap" case
# triggers a sanitizer error.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

python3 - "$WORK_DIR" <<'PYEOF'
import io, sys
import pyarrow as pa
import pyarrow.parquet as pq

work = sys.argv[1]

# --- minimal Thrift compact protocol walker: record offset of every scalar field ---
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
        elif ct == 0x08: self.pos += self.varint()
        elif ct in (0x09, 0x0A): self.lst(path)
        elif ct == 0x0C: self.struct(path)
        else: raise ValueError(f"ctype {ct}")
        if ct in CT_INT: self.rec.append((tuple(path), vs, self.pos, val))
    def lst(self, path):
        st = self.byte(); n = (st & 0xF0) >> 4; et = st & 0x0F
        if n == 0xF: n = self.varint()
        for i in range(n): self.field(et, path + [i])

def first_page_offset(buf):
    col = pq.ParquetFile(io.BytesIO(bytes(buf))).metadata.row_group(0).column(0)
    off = col.data_page_offset
    if col.has_dictionary_page and col.dictionary_page_offset is not None:
        off = min(off, col.dictionary_page_offset)
    return off

def patch(buf, off, field_path, signed_value):
    w = Walker(buf, off); w.struct([])
    for pi, vs, ve, val in w.rec:
        if pi == tuple(field_path):
            nb = encode_varint(zigzag_encode(signed_value))
            assert len(nb) == ve - vs, f"len change for {field_path}: {ve-vs}->{len(nb)} ({val}->{signed_value})"
            buf[vs:ve] = nb
            return buf
    raise KeyError(field_path)

def make(path, dtype, vals, compression="none", use_dictionary=False, data_page_size=None):
    kw = {} if data_page_size is None else {"data_page_size": data_page_size}
    pq.write_table(pa.table({"v": pa.array(vals, type=dtype)}), path,
                   data_page_version="2.0", compression=compression,
                   use_dictionary=use_dictionary, write_statistics=False, **kw)

def page_offsets(buf):
    """Offsets of every data page header (walks header + compressed_page_size), bounded by the
    column chunk's compressed size so we never run into the file footer."""
    col = pq.ParquetFile(io.BytesIO(bytes(buf))).metadata.row_group(0).column(0)
    p = col.data_page_offset
    end = col.data_page_offset + col.total_compressed_size
    offs = []
    while p < end:
        w = Walker(buf, p); w.struct([])
        cps = next(v for (pi, vs, ve, v) in w.rec if pi == (3,))  # compressed_page_size
        offs.append(p)
        p = w.pos + cps
    return offs

# PageHeader field ids: 2=uncompressed_page_size; 8=data_page_header_v2 struct,
# whose 5=definition_levels_byte_length, 6=repetition_levels_byte_length.
NINT = [i if i % 3 else None for i in range(64)]
NSTR = [("x" * ((i % 7) + 1) if i % 3 else None) for i in range(64)]
ARR  = [list(range(i % 5)) for i in range(64)]

def craft(name, dtype, vals, patches, compression="none", use_dictionary=False):
    base = f"{work}/{name}.parquet"
    make(base, dtype, vals, compression, use_dictionary)
    buf = bytearray(open(base, "rb").read())
    off = first_page_offset(buf)
    for fp, v in patches:
        patch(buf, off, fp, v)
    open(f"{work}/{name}_evil.parquet", "wb").write(buf)

# Wrapping bypass (def + rep wraps to a small value -> huge def/rep span -> OOB read).
craft("nint_wrap", pa.int32(), NINT, [((8, 5), -1), ((8, 6), 1)])
craft("nstr_wrap", pa.string(), NSTR, [((8, 5), -2), ((8, 6), 2)])
craft("arr_wrap", pa.list_(pa.int32()), ARR, [((8, 5), 1), ((8, 6), -1)])
# Negative definition length alone (no wrap, must still be rejected).
craft("nint_negdef", pa.int32(), NINT, [((8, 5), -1)])
# Negative uncompressed_page_size on a compressed page (sizes the decompression buffer).
craft("nint_neguncomp", pa.int32(), NINT, [((2,), -8192)], compression="snappy")
# Out-of-range enum values (loading an unscoped enum out of range is undefined behavior).
# PageHeader field 1 = type (PageType); data_page_header_v2 field 4 = encoding (Encoding).
craft("nint_badtype", pa.int32(), NINT, [((1,), -64)])
craft("nint_badenc", pa.int32(), NINT, [((8, 4), -64)])
# Negative num_values (data_page_header_v2 field 1).
craft("nint_negnumvals", pa.int32(), NINT, [((8, 1), -8192)])
# Negative num_values in the DICTIONARY page header (dictionary_page_header field 1). The first page
# of a dictionary-encoded column is the DICTIONARY_PAGE, so this patches its header.
DICTSTR = [("v%d" % (i % 5)) for i in range(64)]  # few distinct values -> small dict num_values
craft("strdict_negnumvals", pa.string(), DICTSTR, [((7, 1), -64)], use_dictionary=True)
# Negative num_rows in a NON-FIRST DataPageV2. num_rows is consumed early (before the num_values
# check) to compute the row cursor, so a negative value on a later page wraps it backwards. Build a
# multi-page file and patch the second page's num_rows (-1024 keeps the 2-byte varint length of 1024).
make(f"{work}/multipage.parquet", pa.int32(), list(range(4000)), data_page_size=256)
mp = bytearray(open(f"{work}/multipage.parquet", "rb").read())
offs = page_offsets(mp)
assert len(offs) >= 2, f"expected multiple pages, got {len(offs)}"
patch(mp, offs[1], (8, 3), -1024)
open(f"{work}/multipage_negnrows_evil.parquet", "wb").write(mp)
PYEOF

# Each crafted file must be rejected with INCORRECT_DATA (code 117), not crash or leak.
for f in nint_wrap nstr_wrap arr_wrap nint_negdef nint_neguncomp nint_badtype nint_badenc nint_negnumvals strdict_negnumvals multipage_negnrows; do
    out=$(${CLICKHOUSE_LOCAL} --query "
        SELECT * FROM file('${WORK_DIR}/${f}_evil.parquet', Parquet) FORMAT Null
        SETTINGS input_format_parquet_use_native_reader_v3 = 1" 2>&1)
    if echo "$out" | grep -q "INCORRECT_DATA"; then
        echo "$f: rejected"
    else
        echo "$f: UNEXPECTED: $out"
    fi
done

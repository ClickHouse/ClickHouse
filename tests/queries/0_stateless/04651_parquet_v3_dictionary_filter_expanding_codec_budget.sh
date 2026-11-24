#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

# Regression test for the compressed carrier of the double-counting bug that `04616` fixed for the
# `UNCOMPRESSED` one. Before decoding a dictionary page on the pruning path, the reader bounds its
# decoded footprint with `Dictionary::decodedFootprintUpperBound` and reserves it against
# `input_format_parquet_memory_high_watermark`. For a compressed column chunk the bytes the decoded
# dictionary owns are the *decompressed* ones (`decompressed_buf`, sized `uncompressed_page_size`);
# the compressed frame stays in the prefetch buffer (`dictionary_page_prefetch`) and is already
# charged to the pruning stage there. The bound used to start from
# `max(uncompressed_page_size, compressed_page_size)`, so whenever the codec expanded the page -
# perfectly legal, and our own writer does not fall back to `UNCOMPRESSED` when compression grows the
# payload - the compressed frame was charged twice and a row group whose decoded dictionary
# comfortably fits the budget fell back to a full scan.
#
# A real codec only expands an incompressible page by a fraction of a percent, too little to pin a
# watermark between the two bounds, so the file below is crafted instead: the dictionary page is
# padded with bytes that belong to the page (`compressed_page_size` covers them, and the reader does
# prefetch them) but that the `ZSTD` frame does not need, which pushes `compressed_page_size` far
# past `uncompressed_page_size` while the file stays perfectly readable. The fixed bound charges only
# the ~1.2 MB decompressed payload and prunes; the double-counting bound demanded twice the ~16 MB
# padded frame on top of the prefetch that already holds it, and fell back to a full scan.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

DATA_FILE="${WORK_DIR}/plain.parquet"
EVIL_FILE="${WORK_DIR}/padded.parquet"

# One fully dictionary-encoded `ZSTD`-compressed string column in a single row group: 60000 distinct
# 16-character values, each appearing twice, so the PLAIN dictionary page payload is
# 60000 * (4 + 16) = 1.2 MB. Checksums, the page index and the bloom filter are disabled so that the
# only offsets to fix up after the padding is inserted are the ones in `ColumnMetaData`.
${CLICKHOUSE_LOCAL} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select concat('v', leftPad(toString(number % 60000), 15, '0')) as s
    from numbers(120000)
    settings output_format_parquet_row_group_size = 1000000, output_format_parquet_max_dictionary_size = 100000000,
        output_format_parquet_compression_method = 'zstd', output_format_parquet_write_checksums = 0,
        output_format_parquet_write_page_index = 0, output_format_parquet_write_bloom_filter = 0,
        engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

python3 - "${DATA_FILE}" "${EVIL_FILE}" <<'PYEOF'
import struct, sys

src, dst = sys.argv[1], sys.argv[2]
PADDING = 16_000_000

# --- minimal Thrift compact protocol walker (records the byte range of every scalar integer field) ---
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
        vs = self.pos
        if ct in (0x01, 0x02): pass
        elif ct == 0x03: self.byte()
        elif ct in (0x04, 0x05, 0x06): self.zz()
        elif ct == 0x07: self.pos += 8
        elif ct == 0x08:
            slen = self.varint(); self.pos += slen
        elif ct in (0x09, 0x0A): self.lst(path)
        elif ct == 0x0C: self.struct(path)
        else: raise ValueError(f"ctype {ct}")
        if ct in CT_INT: self.rec.append((tuple(path), vs, self.pos))
    def lst(self, path):
        st = self.byte(); n = (st & 0xF0) >> 4; et = st & 0x0F
        if n == 0xF: n = self.varint()
        for i in range(n): self.field(et, path + [i])

def walk(buf, off):
    w = Walker(buf, off); w.struct([])
    return {p: (s, e) for p, s, e in w.rec}, w.pos

def value_at(buf, span): return zigzag_decode(Walker(buf, span[0]).varint())

def splice(buf, edits):
    out = bytearray(buf)
    for s, e, nb in sorted(edits, key=lambda t: -t[0]):
        out[s:e] = nb
    return bytes(out)

buf = open(src, "rb").read()

# The footer is [FileMetaData][4-byte little-endian length]["PAR1"].
footer_len = struct.unpack("<I", buf[-8:-4])[0]
footer_start = len(buf) - 8 - footer_len
meta, _ = walk(buf, footer_start)

# The single column chunk of the single row group: `row_groups[0].columns[0].meta_data`.
CM = (4, 0, 1, 0, 3)
total_compressed = value_at(buf, meta[CM + (7,)])
data_page_offset = value_at(buf, meta[CM + (9,)])
dict_page_offset = value_at(buf, meta[CM + (11,)])

# The dictionary page header sits at `dictionary_page_offset`; inflate its `compressed_page_size`
# (field 3) to cover the padding inserted right after the page.
page, page_end = walk(buf, dict_page_offset)
uncompressed_page_size = value_at(buf, page[(2,)])
compressed_page_size = value_at(buf, page[(3,)])
assert (7, 1) in page, "expected a dictionary page header"
assert page_end + compressed_page_size == data_page_offset, \
    "expected the dictionary page to be immediately followed by the first data page"
assert 1_100_000 <= uncompressed_page_size <= 1_300_000, \
    f"unexpected dictionary page payload size: {uncompressed_page_size}"

new_size_bytes = encode_varint(zigzag_encode(compressed_page_size + PADDING))
# The dictionary page grows by the padding plus whatever the re-encoded size field added to the
# header, which moves the first data page and grows the chunk by the same amount. The reader's
# `data_pages_bytes` (`total_compressed_size - (data_page_offset - dictionary_page_offset)`) and
# `dictionary_page_offset` itself are unchanged.
grow = PADDING + len(new_size_bytes) - (page[(3,)][1] - page[(3,)][0])

new_footer = splice(buf[footer_start:], [
    (meta[CM + (7,)][0] - footer_start, meta[CM + (7,)][1] - footer_start,
     encode_varint(zigzag_encode(total_compressed + grow))),
    (meta[CM + (9,)][0] - footer_start, meta[CM + (9,)][1] - footer_start,
     encode_varint(zigzag_encode(data_page_offset + grow))),
])

out = (
    splice(buf[:data_page_offset], [(page[(3,)][0], page[(3,)][1], new_size_bytes)])
    + b"\0" * PADDING
    + buf[data_page_offset:footer_start]
    + new_footer
    + struct.pack("<I", len(new_footer))
    + b"PAR1"
)
open(dst, "wb").write(out)
PYEOF

# Isolate the dictionary filter and keep it always applicable.
CH="${CLICKHOUSE_LOCAL} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0 --input_format_parquet_dictionary_filter_push_down=100000000"

run() {
    local watermark="$1"
    local value="$2"
    ${CH} --input_format_parquet_memory_high_watermark="${watermark}" --query="
        select count() from file('${EVIL_FILE}', Parquet) where s = '${value}' FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

echo "the crafted file reads back exactly like the original"
${CLICKHOUSE_LOCAL} --query="
    select (select (groupBitXor(cityHash64(s)), count()) from file('${DATA_FILE}', Parquet))
         = (select (groupBitXor(cityHash64(s)), count()) from file('${EVIL_FILE}', Parquet))"

echo "generous memory budget: the dictionary filter prunes the row group, 0 rows are read"
run 4000000000 "no_such_value"

echo "moderate memory budget: the ~16 MB padded frame is charged once, by the prefetch, and the ~2.9 MB decoded dictionary fits on top of it, so pruning must still happen; charging the frame a second time demanded ~32 MB more and fell back to a full scan"
run 170000000 "no_such_value"

echo "extreme memory budget (1 byte): pruning is skipped, result is still correct"
run 1 "no_such_value"

echo "results are identical regardless of the memory budget for a value that is present"
diff \
    <(run 4000000000 "v000000000000123") \
    <(run 1 "v000000000000123") \
    && echo "OK"

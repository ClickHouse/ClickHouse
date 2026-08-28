#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

# `compressed_page_size` is the number of bytes a Parquet page owns, not necessarily the exact
# length of the codec frame: a writer may pad the page. The gzip reader validates the gzip trailer
# of every member, and it must do so without mistaking such padding for another gzip member -
# a padded page is a valid file and must stay readable. See also
# `04651_parquet_v3_dictionary_filter_expanding_codec_budget`, which relies on the same shape for
# `ZSTD`, and `04848_parquet_gzip_corrupted_trailer`, which checks the corrupted trailer is caught.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

DATA_FILE="${WORK_DIR}/plain.parquet"
PADDED_FILE="${WORK_DIR}/padded.parquet"
EMPTY_MEMBER_FILE="${WORK_DIR}/empty-member.parquet"
MAGIC_PADDING_FILE="${WORK_DIR}/magic-padding.parquet"
OVERFLOW_FILE="${WORK_DIR}/overflow.parquet"
SPLIT_MEMBER_FILE="${WORK_DIR}/split-member.parquet"
TRUNCATED_TRAILER_FILE="${WORK_DIR}/truncated-trailer.parquet"

# One fully dictionary-encoded `gzip`-compressed string column in a single row group. Checksums, the
# page index and the bloom filter are disabled so that the only offsets to fix up after the padding
# is inserted are the ones in `ColumnMetaData`.
${CLICKHOUSE_LOCAL} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select concat('v', leftPad(toString(number % 60000), 15, '0')) as s
    from numbers(120000)
    settings output_format_parquet_row_group_size = 1000000, output_format_parquet_max_dictionary_size = 100000000,
        output_format_parquet_compression_method = 'gzip', output_format_parquet_write_checksums = 0,
        output_format_parquet_write_page_index = 0, output_format_parquet_write_bloom_filter = 0,
        engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

python3 - "${DATA_FILE}" "${PADDED_FILE}" "${EMPTY_MEMBER_FILE}" "${MAGIC_PADDING_FILE}" "${OVERFLOW_FILE}" "${SPLIT_MEMBER_FILE}" "${TRUNCATED_TRAILER_FILE}" <<'PYEOF'
import gzip, struct, sys

src, padded_dst, empty_member_dst, magic_padding_dst, overflow_dst, split_member_dst, truncated_trailer_dst = sys.argv[1:]
PADDING = 4096

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

def make_file(dst, replacement):
    new_size_bytes = encode_varint(zigzag_encode(len(replacement)))
    # The dictionary page grows by the replacement plus whatever the re-encoded size field added to the
    # header, which moves the first data page and grows the chunk by the same amount. The reader's
    # `data_pages_bytes` (`total_compressed_size - (data_page_offset - dictionary_page_offset)`) and
    # `dictionary_page_offset` itself are unchanged.
    grow = len(replacement) - compressed_page_size + len(new_size_bytes) - (page[(3,)][1] - page[(3,)][0])

    new_footer = splice(buf[footer_start:], [
        (meta[CM + (7,)][0] - footer_start, meta[CM + (7,)][1] - footer_start,
         encode_varint(zigzag_encode(total_compressed + grow))),
        (meta[CM + (9,)][0] - footer_start, meta[CM + (9,)][1] - footer_start,
         encode_varint(zigzag_encode(data_page_offset + grow))),
    ])

    out = (
        splice(buf[:page_end], [(page[(3,)][0], page[(3,)][1], new_size_bytes)])
        + replacement
        + buf[data_page_offset:footer_start]
        + new_footer
        + struct.pack("<I", len(new_footer))
        + b"PAR1"
    )
    open(dst, "wb").write(out)

compressed_payload = buf[page_end:data_page_offset]
make_file(padded_dst, compressed_payload + b"\0" * PADDING)
# A trailing empty member must be accepted, while padding that merely starts with the gzip magic
# bytes must not be mistaken for a malformed member.
make_file(empty_member_dst, compressed_payload + gzip.compress(b""))
make_file(magic_padding_dst, compressed_payload + b"\x1f\x8bnot-a-gzip-member")
# A second gzip member must not be treated as page padding after the first member filled the
# declared output exactly: it expands past the page header and must be rejected.
make_file(overflow_dst, compressed_payload + gzip.compress(b"overflow"))

# A page payload may itself be a sequence of gzip members. Splitting the dictionary payload proves
# that a member ending before the declared page size resets `inflate` and continues with the next.
plain_payload = gzip.decompress(compressed_payload)
split_at = len(plain_payload) // 2
make_file(split_member_dst, gzip.compress(plain_payload[:split_at]) + gzip.compress(plain_payload[split_at:]))

# The opposite of padding: the page ends right after the DEFLATE payload, with the 8-byte gzip
# trailer (`CRC32` + `ISIZE`) cut off. The payload fills the declared output exactly, so the
# missing trailer is only noticed while validating it, and the page must be rejected cleanly.
make_file(truncated_trailer_dst, compressed_payload[:-8])
PYEOF

echo "the padded file reads back exactly like the original"
${CLICKHOUSE_LOCAL} --query="
    select (select (groupBitXor(cityHash64(s)), count()) from file('${DATA_FILE}', Parquet))
         = (select (groupBitXor(cityHash64(s)), count()) from file('${PADDED_FILE}', Parquet))"

echo "an empty trailing member and magic-prefixed padding read back exactly"
${CLICKHOUSE_LOCAL} --query="
    select (select (groupBitXor(cityHash64(s)), count()) from file('${DATA_FILE}', Parquet))
         = (select (groupBitXor(cityHash64(s)), count()) from file('${EMPTY_MEMBER_FILE}', Parquet))
       and (select (groupBitXor(cityHash64(s)), count()) from file('${DATA_FILE}', Parquet))
         = (select (groupBitXor(cityHash64(s)), count()) from file('${MAGIC_PADDING_FILE}', Parquet))"

echo "members that collectively fill the page read back exactly"
${CLICKHOUSE_LOCAL} --query="
    select (select (groupBitXor(cityHash64(s)), count()) from file('${DATA_FILE}', Parquet))
         = (select (groupBitXor(cityHash64(s)), count()) from file('${SPLIT_MEMBER_FILE}', Parquet))"

echo "a page whose gzip trailer is cut off is rejected"
${CLICKHOUSE_LOCAL} --query="select sum(length(s)) from file('${TRUNCATED_TRAILER_FILE}', Parquet)" 2>&1 | grep -c 'the gzip member is not properly terminated'

echo "a second gzip member beyond the declared output is rejected"
${CLICKHOUSE_LOCAL} --query="select sum(length(s)) from file('${OVERFLOW_FILE}', Parquet)" 2>&1 | grep -c 'Compressed page uncompresses to more than the declared'

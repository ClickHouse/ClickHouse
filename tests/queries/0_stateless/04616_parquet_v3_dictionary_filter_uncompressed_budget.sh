#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs pyarrow to craft the Parquet file, and the Parquet format which is not built in
# fasttest.

# Regression test: the dictionary-filter pruning path bounds the decoded dictionary's memory before
# decoding it (`Dictionary::decodedFootprintUpperBound`). For an `UNCOMPRESSED` column chunk the page
# payload is *not* materialized in `decompressed_buf` - `Dictionary::data` points straight into the
# prefetched page buffer, whose bytes are already charged to the pruning stage by the prefetcher - so
# the bound must not charge the page payload again. The old bound started from
# `uncompressed_page_size` unconditionally, double-counting the page against
# `input_format_parquet_memory_high_watermark` and disabling pruning on uncompressed files whose real
# incremental decoded state (here: the `StringPlain` offsets and the value-set hashes) fits the budget.
#
# To pin the removed term down deterministically, the crafted file inflates the dictionary page
# header's `uncompressed_page_size` (a field the reader never uses on the `UNCOMPRESSED` path, where
# the payload span comes from `compressed_page_size`) far above the watermark: the fixed bound ignores
# it and prunes, while the double-counting bound reserved `2 * uncompressed_page_size` and fell back
# to a full scan.

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

def patch(buf, off, field_path, signed_value):
    w = Walker(buf, off); w.struct([])
    for pi, vs, ve, val in w.rec:
        if pi == tuple(field_path):
            nb = encode_varint(zigzag_encode(signed_value))
            assert len(nb) == ve - vs, f"len change for {field_path}: {ve-vs}->{len(nb)} ({val}->{signed_value})"
            buf[vs:ve] = nb
            return val
    raise KeyError(field_path)

# One fully dictionary-encoded, UNCOMPRESSED string column in a single row group: 60000 distinct
# 16-character values, each appearing twice, so the on-disk PLAIN dictionary page is 60000 * (4 + 16)
# = 1.2 MB and the queried value 'no_such_value' is provably absent (the row group is prunable).
vals = [f"v{i:015d}" for i in range(60000)] * 2
base = f"{work}/uncompressed.parquet"
pq.write_table(pa.table({"s": pa.array(vals, type=pa.string())}), base,
               compression="NONE", data_page_version="2.0", use_dictionary=True,
               dictionary_pagesize_limit=10_000_000, write_statistics=False)

# The dictionary page is the first page of the only column chunk, so its Thrift `PageHeader` starts
# right after the 4-byte "PAR1" magic. Inflate `uncompressed_page_size` (field 2) to ~100 MB; for the
# UNCOMPRESSED codec the reader takes the payload span from `compressed_page_size` (field 3) and never
# reads this field, so the file stays readable, but a footprint bound that wrongly charges the page
# payload now demands ~200 MB from the watermark.
buf = bytearray(open(base, "rb").read())
old = patch(buf, 4, (2,), 100_000_000)
assert 1_100_000 <= old <= 1_300_000, f"unexpected dictionary page size: {old}"
open(f"{work}/uncompressed_evil.parquet", "wb").write(buf)
PYEOF

# Isolate the dictionary filter and keep it always applicable.
CH="${CLICKHOUSE_LOCAL} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0 --input_format_parquet_dictionary_filter_push_down=100000000"

run() {
    local watermark="$1"
    local value="$2"
    ${CH} --input_format_parquet_memory_high_watermark="${watermark}" --query="
        select count() from file('${WORK_DIR}/uncompressed_evil.parquet', Parquet) where s = '${value}' FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

echo "generous memory budget: the dictionary filter prunes the row group, 0 rows are read"
run 4000000000 "no_such_value"

echo "moderate memory budget (50 MB): the real incremental footprint fits and pruning must still happen; charging the inflated uncompressed page size for the prefetch-backed payload would demand ~200 MB and fall back to a full scan"
run 50000000 "no_such_value"

echo "extreme memory budget (1 byte): pruning is skipped, result is still correct"
run 1 "no_such_value"

echo "results are identical regardless of the memory budget for a value that is present"
diff \
    <(run 4000000000 "v000000000000123") \
    <(run 1 "v000000000000123") \
    && echo "OK"

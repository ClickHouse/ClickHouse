#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

# A negative ColumnMetaData.num_values, total_uncompressed_size or size_statistics.unencoded_byte_array_data_bytes
# in a column chunk whose data the query reads is rejected with INCORRECT_DATA. A positive one implying 2^61 bytes
# or more is only capped or ignored as a memory estimate, so the file still reads correctly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

write() { $CLICKHOUSE_LOCAL -q "SELECT $2 FROM numbers(100) FORMAT Parquet" > "$WORK_DIR/$1.parquet"; }
write i64 "number AS x"
write arr "[number, number + 1] AS x"
write str "concat('s', toString(number)) AS x"
write nul "if(number % 10 = 0, NULL, number) AS x"
write wide "number AS a, number + 1 AS b, number + 2 AS c, number + 3 AS d, number + 4 AS e"

python3 - "$WORK_DIR" <<'PYEOF'
import struct, sys
work = sys.argv[1]

def varint(buf, pos):
    r = s = 0
    while True:
        b = buf[pos]; pos += 1; r |= (b & 0x7F) << s; s += 7
        if not b & 0x80: return r, pos

def walk(buf, pos, ct, path, out):
    """Skips one Thrift compact protocol value of type `ct`; out[path] = (value start, end, int value)."""
    start, val = pos, None
    if ct == 3: pos += 1
    elif ct in (4, 5, 6): u, pos = varint(buf, pos); val = (u >> 1) ^ -(u & 1)
    elif ct == 7: pos += 8
    elif ct == 8: n, pos = varint(buf, pos); pos += n
    elif ct in (9, 10):
        h = buf[pos]; pos += 1; n = h >> 4
        if n == 15: n, pos = varint(buf, pos)
        for i in range(n): pos = pos + 1 if h & 15 in (1, 2) else walk(buf, pos, h & 15, path + (i,), out)
    elif ct == 12:
        fid = 0
        while buf[pos]:
            h = buf[pos]; pos += 1
            if h >> 4: fid += h >> 4
            else: u, pos = varint(buf, pos); fid = (u >> 1) ^ -(u & 1)
            pos = walk(buf, pos, h & 15, path + (fid,), out)
        pos += 1
    elif ct not in (1, 2): raise ValueError(ct)
    out[path] = (start, pos, val)
    return pos

def rewrite(name, base, patches):
    buf = open(f"{work}/{base}.parquet", "rb").read()
    off = len(buf) - 8 - struct.unpack("<I", buf[-8:-4])[0]
    footer = buf[off:-8]
    for path, value in patches:
        out = {}; walk(footer, 0, 12, (), out)
        start, end, cur = out[path]
        assert cur > 0, (name, path, cur)
        u, enc = ((value << 1) ^ (value >> 63)) & (2 ** 64 - 1), bytearray()
        while True:
            enc.append((u & 0x7F) | (0x80 if u >> 7 else 0)); u >>= 7
            if not u: break
        footer = footer[:start] + bytes(enc) + footer[end:]
    open(f"{work}/{name}.parquet", "wb").write(buf[:off] + footer + struct.pack("<I", len(footer)) + b"PAR1")

# FileMetaData.row_groups[0].columns[c].meta_data: num_values 5, total_uncompressed_size 6, statistics 12
# (null_count 3), size_statistics 16 (unencoded_byte_array_data_bytes 1).
M = lambda c, *f: (4, 0, 1, c, 3) + f
rewrite("i64_neg", "i64", [(M(0, 5), -312904)])
rewrite("i64_size_neg", "i64", [(M(0, 6), -1)])
rewrite("str_bytes_neg", "str", [(M(0, 16, 1), -1)])
rewrite("nul_neg", "nul", [(M(0, 5), -5), (M(0, 12, 3), -5)])
rewrite("nul_huge", "nul", [(M(0, 5), 2 ** 62), (M(0, 12, 3), 2 ** 62)])
rewrite("i64_huge", "i64", [(M(0, 5), 2 ** 62)])
rewrite("str_bytes_huge", "str", [(M(0, 16, 1), 2 ** 63 - 1)])
rewrite("arr_huge", "arr", [(M(0, 5), 2 ** 60)])
rewrite("wide_huge", "wide", [(M(c, 5), 2 ** 62) for c in range(5)])
rewrite("wide_neg", "wide", [(M(1, 5), -1)])
PYEOF

read_file() { $CLICKHOUSE_LOCAL "${@:3}" -q "SELECT count(), sum(cityHash64(*)) FROM file('$WORK_DIR/$1.parquet', Parquet) $2" 2>&1; }
negative() {
    out=$(read_file "$1" "$2")
    echo "$1: $(grep -o -m1 'has a negative value count or size' <<< "$out") $(grep -o -m1 'INCORRECT_DATA' <<< "$out")"
}
for f in i64_neg i64_size_neg str_bytes_neg; do negative $f ""; done
# A row group pruned by statistics is not read, so it is not checked.
echo "i64_neg pruned: $(read_file i64_neg "WHERE x > 1000")"
# Nor is a column that PREWHERE leaves with no rows to read.
echo "wide_neg prewhere: $(read_file wide_neg "PREWHERE cityHash64(a) = 0")"
# A corrupt num_values = null_count must not silently prune the row group.
negative nul_neg "WHERE x = 5"
for f in nul nul_huge; do echo "$f: $(read_file $f "WHERE x = 5")"; done

for f in i64 i64_huge str str_bytes_huge arr arr_huge; do echo "$f: $(read_file $f)"; done
# With this low watermark, the capped per-column estimates still add up past the signed range.
for f in wide wide_huge; do echo "$f: $(read_file $f "" --input_format_parquet_memory_low_watermark=9223372036854775808)"; done

#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

# The Parquet reader derives its memory estimates and pre-allocations from ColumnMetaData.num_values,
# total_uncompressed_size and size_statistics.unencoded_byte_array_data_bytes. A negative value must be
# rejected with INCORRECT_DATA. An estimate above the reader's memory budget (the larger memory watermark,
# at most 2^61 bytes) must be ignored, so such a file still reads correctly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

write() { $CLICKHOUSE_LOCAL --output_format_parquet_max_dictionary_size="$2" -q "SELECT $3 FROM numbers(100) FORMAT Parquet" > "$WORK_DIR/$1.parquet"; }
write i64 0 "number AS x"
write arr 0 "[number, number + 1] AS x"
write str 0 "concat('s', toString(number)) AS x"
write strdict 1048576 "concat('v', toString(number % 4)) AS x"
write nul 0 "if(number % 10 = 0, NULL, number) AS x"
write wide 0 "number AS a, number + 1 AS b, number + 2 AS c, number + 3 AS d, number + 4 AS e"

python3 - "$WORK_DIR" <<'PYEOF'
import struct, sys
work = sys.argv[1]

def varint(buf, pos):
    r = s = 0
    while True:
        b = buf[pos]; pos += 1; r |= (b & 0x7F) << s; s += 7
        if not b & 0x80: return r, pos

def walk(buf, pos, ct, path, out):
    """Skips one Thrift compact protocol value of type `ct`; out[path] = [value start, end, int value, field header]."""
    start, val = pos, None
    if ct == 3: val, pos = buf[pos], pos + 1
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
            header, h = pos, buf[pos]; pos += 1
            if h >> 4: fid += h >> 4
            else: u, pos = varint(buf, pos); fid = (u >> 1) ^ -(u & 1)
            pos = walk(buf, pos, h & 15, path + (fid,), out)
            out[path + (fid,)][3] = header
        pos += 1
    elif ct not in (1, 2): raise ValueError(ct)
    out[path] = [start, pos, val, None]
    return pos

def rewrite(name, base, patches, drop=(), old=None):
    buf = open(f"{work}/{base}.parquet", "rb").read()
    off = len(buf) - 8 - struct.unpack("<I", buf[-8:-4])[0]
    footer = buf[off:-8]
    for path in drop:  # removes a whole field, which must be the last one of its struct
        out = {}; walk(footer, 0, 12, (), out)
        _, end, _, header = out[path]
        assert footer[end] == 0, (name, path)
        footer = footer[:header] + footer[end:]
    for path, value in patches:
        out = {}; walk(footer, 0, 12, (), out)
        start, end, cur, _ = out[path]
        assert old(path, cur), (name, path, cur)
        u, enc = ((value << 1) ^ (value >> 63)) & (2 ** 64 - 1), bytearray()
        while True:
            enc.append((u & 0x7F) | (0x80 if u >> 7 else 0)); u >>= 7
            if not u: break
        footer = footer[:start] + bytes(enc) + footer[end:]
    open(f"{work}/{name}.parquet", "wb").write(buf[:off] + footer + struct.pack("<I", len(footer)) + b"PAR1")

# FileMetaData.row_groups[0].columns[c].meta_data: num_values 5, total_uncompressed_size 6, statistics 12
# (null_count 3), size_statistics 16 (unencoded_byte_array_data_bytes 1).
M = lambda c=0, *f: (4, 0, 1, c, 3) + f
rows = lambda n: lambda p, cur: cur == n
positive = lambda p, cur: cur > 0
rewrite("i64_neg", "i64", [(M(0, 5), -312904)], old=rows(100))
rewrite("arr_neg", "arr", [(M(0, 5), -1)], old=rows(200))
rewrite("str_bytes_neg", "str", [(M(0, 16, 1), -1)], old=positive)
rewrite("str_size_neg", "str", [(M(0, 6), -1)], drop=[M(0, 16)], old=positive)
rewrite("nul_neg", "nul", [(M(0, 5), -5), (M(0, 12, 3), -5)], old=lambda p, cur: cur == (100 if p[-1] == 5 else 10))
rewrite("i64_huge", "i64", [(M(0, 5), 2 ** 62)], old=rows(100))
rewrite("str_bytes_huge", "str", [(M(0, 16, 1), 2 ** 63 - 1)], old=positive)
rewrite("str_size_huge", "str", [(M(0, 6), 2 ** 63 - 1)], drop=[M(0, 16)], old=positive)
rewrite("arr_huge", "arr", [(M(0, 5), 2 ** 60)], old=rows(200))
rewrite("arr_4g", "arr", [(M(0, 5), 2 ** 32)], old=rows(200))
rewrite("arr_2e59", "arr", [(M(0, 5), 2 ** 59)], old=rows(200))
rewrite("strdict_huge", "strdict", [(M(0, 5), 2 ** 62)], drop=[M(0, 16)], old=rows(100))
rewrite("wide_huge", "wide", [(M(c, 5), 2 ** 62) for c in range(5)], old=rows(100))
rewrite("wide_neg", "wide", [(M(1, 5), -1)], old=rows(100))
PYEOF

read_file() { $CLICKHOUSE_LOCAL "${@:3}" -q "SELECT count(), sum(cityHash64(*)) FROM file('$WORK_DIR/$1.parquet', Parquet) $2" 2>&1; }
negative() {
    out=$(read_file "$1" "$2")
    echo "$1: $(grep -o -m1 'has a negative value count or size' <<< "$out") $(grep -o -m1 'INCORRECT_DATA' <<< "$out")"
}
declare -A expected
for f in i64 arr str strdict wide; do expected[$f]=$(read_file $f ""); done
same() {
    actual=$(read_file "$1" "" "${@:3}")
    if [ "$actual" = "${expected[$2]}" ]; then echo "$1${3:+ $3}: same as $2"; else echo "$1${3:+ $3}: $actual (expected ${expected[$2]})"; fi
}

echo "${expected[i64]}"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$WORK_DIR/i64_neg.parquet', Parquet)"
for f in i64_neg arr_neg str_bytes_neg str_size_neg; do negative $f ""; done
negative nul_neg "WHERE x = 5"

# Only the columns a query reads are validated.
cols() { $CLICKHOUSE_LOCAL -q "SELECT count(), sum(cityHash64(a, c)) FROM file('$WORK_DIR/$1.parquet', Parquet)" 2>&1; }
if [ "$(cols wide_neg)" = "$(cols wide)" ]; then echo "wide_neg a, c: same as wide"; else echo "wide_neg a, c: $(cols wide_neg) (expected $(cols wide))"; fi
negative wide_neg ""

# The low watermark, unlike the high one, is not capped by the server memory limit.
low=--input_format_parquet_memory_low_watermark
for setting in "" "$low=9223372036854775808"; do
    same i64_huge i64 $setting
    same str_bytes_huge str $setting
    same str_size_huge str $setting
    same arr_huge arr $setting
    same strdict_huge strdict $setting
done
for setting in "$low=9223372036854775808" "$low=18446744073709551615"; do same wide_huge wide $setting; done
same arr_2e59 arr "$low=9223372036854775808"
same arr_4g arr "$low=8589934592" --max_memory_usage=8000000000

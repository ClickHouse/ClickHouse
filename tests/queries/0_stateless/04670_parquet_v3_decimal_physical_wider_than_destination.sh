#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs pyarrow to craft the fixtures, and Parquet is not built in fasttest.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DATA"
mkdir -p "$DATA"
trap 'rm -rf "$DATA"' EXIT

# Every fixture declares a DECIMAL precision that maps to a narrower ClickHouse type than one
# encoded value occupies (INT64 or FIXED_LEN_BYTE_ARRAY with precision 9 -> Decimal32). Reading
# any of them used to write past the end of the destination column.
python3 - "$DATA" <<'PYEOF'
import decimal
import os
import struct
import sys

import pyarrow as pa
import pyarrow.parquet as pq

OUT = sys.argv[1]

# pyarrow always declares a precision that matches the physical width, so write the wide precision
# and then rewrite the footer's compact-thrift DecimalType {1: i32 scale, 2: i32 precision} varint
# pair in place. The replacement is the same byte width, so every offset in the file stays valid.
SCALE2 = bytes([0x15, 0x04])
PREC = {9: bytes([0x15, 0x12]), 18: bytes([0x15, 0x24]), 38: bytes([0x15, 0x4C])}


def patch_precision(path, was, now):
    b = bytearray(open(path, "rb").read())
    assert b[:4] == b"PAR1" and b[-4:] == b"PAR1", "not a parquet file"
    flen = struct.unpack("<I", b[-8:-4])[0]
    start = len(b) - 8 - flen
    foot = b[start:start + flen]
    assert foot.count(SCALE2 + PREC[was]) >= 1, f"declared precision {was} absent in {path}"
    b[start:start + flen] = foot.replace(SCALE2 + PREC[was], SCALE2 + PREC[now])
    open(path, "wb").write(bytes(b))


def write(name, values, *, precision, use_dictionary, as_integer, physical, type_length,
          row_group_size=None, row_groups=1, declare=None):
    path = os.path.join(OUT, name + ".parquet")
    kw = {} if row_group_size is None else {"row_group_size": row_group_size}
    pq.write_table(pa.table({"k": pa.array(values, type=pa.decimal128(precision, 2))}), path,
                   use_dictionary=use_dictionary, compression="none", version="2.6",
                   store_decimal_as_integer=as_integer, data_page_size=1 << 20,
                   write_statistics=True, **kw)
    if declare is not None:
        patch_precision(path, precision, declare)
    # Read every property the test relies on back out of the file: the write options only request
    # them, and the precision patch is a blind byte replace.
    f = pq.ParquetFile(path)
    c = f.schema.column(0)
    want = f"Decimal(precision={declare or precision}, scale=2)"
    assert str(c.logical_type) == want, f"{name}: declared {c.logical_type}, wanted {want}"
    assert c.physical_type == physical, f"{name}: physical {c.physical_type}, wanted {physical}"
    assert c.length == type_length, f"{name}: type_length {c.length}, wanted {type_length}"
    assert f.metadata.num_rows == len(values)
    assert f.metadata.num_row_groups == row_groups, f"{name}: {f.metadata.num_row_groups} row groups"
    for g in range(f.metadata.num_row_groups):
        col_meta = f.metadata.row_group(g).column(0)
        assert col_meta.statistics is not None, f"{name}: rg{g} has no statistics"
        dict_encoded = "RLE_DICTIONARY" in list(col_meta.encodings)
        assert dict_encoded == use_dictionary, f"{name}: rg{g} encodings {list(col_meta.encodings)}"


# 300 rows over 60 distinct values: enough to fill a dictionary page, all within 9 digits so they
# fit Decimal32(9, 2) losslessly.
vals = [decimal.Decimal(f"{(i * 7919) % 100000}.{(i * 37) % 100:02d}") for i in range(60)]
col = [vals[i % 60] for i in range(300)]

# Physical INT64, declared precision 9, on dictionary and on plain pages.
write("int64_dict", col, precision=18, use_dictionary=True, as_integer=True,
      physical="INT64", type_length=0, declare=9)
write("int64_plain", col, precision=18, use_dictionary=False, as_integer=True,
      physical="INT64", type_length=0, declare=9)
# Physical FIXED_LEN_BYTE_ARRAY, declared precision 9, type_length 8 and 16.
write("flba8", col, precision=18, use_dictionary=True, as_integer=False,
      physical="FIXED_LEN_BYTE_ARRAY", type_length=8, declare=9)
write("flba16", col, precision=38, use_dictionary=True, as_integer=False,
      physical="FIXED_LEN_BYTE_ARRAY", type_length=16, declare=9)
# A value needing 12 digits, so it does not fit Decimal32(9, 2).
write("overflow", [decimal.Decimal("9999999999.99")] + col[1:], precision=18,
      use_dictionary=True, as_integer=True, physical="INT64", type_length=0, declare=9)

# Row-group-pruning pair: values increase monotonically, so `k > 500` rules out 8 of the 10 row
# groups. Both files hold identical data and differ only in the declared precision, so the
# mismatched one's inability to prune is a property of the shape, not of the layout.
rg = [decimal.Decimal(f"{i}.{i % 100:02d}") for i in range(600)]
write("rowgroups_mismatch", rg, precision=18, use_dictionary=True, as_integer=True,
      physical="INT64", type_length=0, row_group_size=60, row_groups=10, declare=9)
write("rowgroups_control", rg, precision=18, use_dictionary=True, as_integer=True,
      physical="INT64", type_length=0, row_group_size=60, row_groups=10)
PYEOF

echo '-- dictionary-encoded INT64, declared precision 9'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/int64_dict.parquet', Parquet)"

echo '-- plain-encoded INT64, declared precision 9'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/int64_plain.parquet', Parquet)"

echo '-- FIXED_LEN_BYTE_ARRAY type_length 8, declared precision 9'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/flba8.parquet', Parquet)"

echo '-- FIXED_LEN_BYTE_ARRAY type_length 16, declared precision 9 (skips two width buckets)'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/flba16.parquet', Parquet)"

echo '-- a WHERE over the narrowed column still returns the right rows'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/int64_dict.parquet', Parquet) WHERE k > 50000 SETTINGS input_format_parquet_filter_push_down = 1"

# Statistics decode to a value of the physical width, not the type the key range is built from, so
# they are unusable on this shape. All three fixtures hold 10 row groups of which `k > 500` matches
# the last 2, so the exact counts are the oracle: pruning a matching group changes the split.
prune_counts() {
    $CLICKHOUSE_LOCAL --print-profile-events -q "$1" 2>&1 | awk '
        /ParquetReadRowGroups:/   { read   += $(NF-1) }
        /ParquetPrunedRowGroups:/ { pruned += $(NF-1) }
        END { printf "read=%d pruned=%d\n", read, pruned }'
}

echo '-- physical wider than declared: statistics are not usable, so no row group is pruned'
prune_counts "SELECT count() FROM file('$DATA/rowgroups_mismatch.parquet', Parquet) WHERE k > 500 SETTINGS input_format_parquet_filter_push_down = 1"
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/rowgroups_mismatch.parquet', Parquet) WHERE k > 500 SETTINGS input_format_parquet_filter_push_down = 1"

echo '-- same file well-formed (declared precision 18): pruning still works'
prune_counts "SELECT count() FROM file('$DATA/rowgroups_control.parquet', Parquet) WHERE k > 500 SETTINGS input_format_parquet_filter_push_down = 1"
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/rowgroups_control.parquet', Parquet) WHERE k > 500 SETTINGS input_format_parquet_filter_push_down = 1"

echo '-- a hint of exactly the decoded width restores pruning'
prune_counts "SELECT count() FROM file('$DATA/rowgroups_mismatch.parquet', Parquet, 'k Decimal(18, 2)') WHERE k > 500 SETTINGS input_format_parquet_filter_push_down = 1"
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/rowgroups_mismatch.parquet', Parquet, 'k Decimal(18, 2)') WHERE k > 500 SETTINGS input_format_parquet_filter_push_down = 1"

echo '-- schema inference still reports the declared precision'
$CLICKHOUSE_LOCAL -q "DESC file('$DATA/int64_dict.parquet', Parquet)"

echo '-- explicit wider type hint reads without narrowing'
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(k), count(), sum(k) FROM file('$DATA/int64_dict.parquet', Parquet, 'k Decimal(18, 2)') GROUP BY 1"

echo '-- a value that exceeds the declared precision is an error, not a corrupted read'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/overflow.parquet', Parquet)" 2>&1 | grep -o -m1 'DECIMAL_OVERFLOW'

echo '-- ... and reads losslessly with a wide enough hint'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(k) FROM file('$DATA/overflow.parquet', Parquet, 'k Decimal(18, 2)')"

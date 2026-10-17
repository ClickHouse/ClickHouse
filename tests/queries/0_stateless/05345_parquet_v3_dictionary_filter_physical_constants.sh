#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The dictionary filter of the Parquet v3 reader compares the stored values of an integer dictionary with
# query constants directly instead of hashing every value, each constant at most once: one pass each for the
# first 32, then one pass with a binary search over all the remaining ones. For every integer width,
# signedness and bit pattern it must agree with reading without the filter, and only the row groups that
# hold a value are read. Very many constants take the hashed path.

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
MANY=$(seq -s ', ' 1000 1099)
VERY_MANY=$(seq -s ', ' 1000 1299)

# 4 row groups of 1000 rows. In each row group every column has 4 distinct values (so it is dictionary-encoded)
# that occur in no other row group, covering the sign bit and both extremes of each integer type.
# `max_block_size` is pinned so the data arrives in a single block and the row group boundaries are fixed.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select
        toInt32(multiIf(g = 0, -2147483648 + v, g = 1, -1 - v, g = 2, v, 2147483647 - v)) as i32,
        toUInt32(multiIf(g = 0, v, g = 1, 2147483648 + v, g = 2, 4294967292 + v, 65536 + v)) as u32,
        toInt64(multiIf(g = 0, toInt64(-9223372036854775808) + v, g = 1, toInt64(-1) - v, g = 2, toInt64(4294967296) + v, toInt64(9223372036854775804) + v)) as i64,
        toUInt64(multiIf(g = 0, toUInt64(v), g = 1, toUInt64(9223372036854775808) + toUInt64(v), g = 2, toUInt64(18446744073709551612) + toUInt64(v), toUInt64(4294967295) + toUInt64(v))) as u64,
        if(number % 5 = 0, NULL, toInt64(multiIf(g = 0, -5 - v, g = 1, 5 + v, g = 2, toInt64(-4294967296) - v, toInt64(4294967296) + v))) as n64,
        toInt16(multiIf(g = 0, -32768 + v, g = 1, -1 - v, g = 2, 100 + v, 32767 - v)) as i16,
        toUInt16(multiIf(g = 0, v, g = 1, 32768 + v, g = 2, 65532 + v, 1000 + v)) as u16,
        toInt8(multiIf(g = 0, -128 + v, g = 1, -1 - v, g = 2, 10 + v, 127 - v)) as i8,
        toUInt8(multiIf(g = 0, v, g = 1, 128 + v, g = 2, 252 + v, 20 + v)) as u8,
        toInt64(g * 10 + v) as k
    from (select number, intDiv(number, 1000) as g, toInt64(number % 4) as v from numbers(4000))
    settings output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# Disable the other row-group/page filters so we observe the dictionary filter in isolation.
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0"

LABELS=()
QUERIES=()
# Adds the count of rows matching condition `$2` (default: the label `$1`), reading the file with structure `$3`.
add() {
    LABELS+=("$1${3:+ [$3]}")
    QUERIES+=("select count() from file('${DATA_FILE}', Parquet${3:+, '$3'}) where ${2:-$1} FORMAT JSON")
}

for cond in \
    "i32 = -2147483647" "i32 = -2" "i32 = 1" "i32 = 2147483646" "i32 = 12345" "i32 IN (-2147483648, 2147483647)" \
    "u32 = 1" "u32 = 2147483649" "u32 = 4294967293" "u32 = 65537" "u32 = 2147483647" "u32 IN (2147483648, 4294967295)" \
    "i64 = -9223372036854775807" "i64 = -2" "i64 = 4294967297" "i64 = 9223372036854775805" "i64 = 0" "i64 IN (-9223372036854775808, 9223372036854775807)" \
    "u64 = 1" "u64 = 9223372036854775809" "u64 = 18446744073709551613" "u64 = 4294967296" "u64 = 9223372036854775807" "u64 IN (9223372036854775808, 18446744073709551615)" \
    "n64 = -6" "n64 = 6" "n64 = -4294967297" "n64 = 4294967297" "n64 = 0" "n64 IN (-5, 4294967299)" \
    "i16 = -32767" "i16 = -2" "i16 = 101" "i16 = 32766" "i16 = 0" "i16 IN (-32768, 32767)" \
    "u16 = 1" "u16 = 32769" "u16 = 65535" "u16 = 1001" "u16 = 2000" \
    "i8 = -127" "i8 = -2" "i8 = 11" "i8 = 126" "i8 = 0" "i8 IN (-128, 127)" \
    "u8 = 1" "u8 = 129" "u8 = 255" "u8 = 21" "u8 = 100" \
    "k IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)" "k IN (100, 101, 102, 103, 104, 105, 106, 107, 31)" \
    "k = 100 OR k = 101 OR k = 102 OR k = 103 OR k = 104 OR k = 105 OR k = 106 OR k = 107 OR k = 108 OR k = 21" \
    "k = 100 OR k = 101 OR k = 102 OR k = 103 OR k = 104 OR k = 105 OR k = 106 OR k = 107 OR k = 108 OR k = 109"
do
    add "${cond}"
done

# 100 constants are compared in one pass with a binary search, 300 take the hashed path.
add "k IN (1000..1099, 22)" "k IN (${MANY}, 22)"
add "k IN (1000..1099)" "k IN (${MANY})"
add "k IN (1000..1299, 22)" "k IN (${VERY_MANY}, 22)"
add "k IN (1000..1299)" "k IN (${VERY_MANY})"
add "i32 IN (-2147483647, 1000..1099)" "i32 IN (-2147483647, ${MANY})"
add "u64 IN (18446744073709551613, 1000..1099)" "u64 IN (18446744073709551613, ${MANY})"
add "i16 IN (-32767, 1000..1099)" "i16 IN (-32767, ${MANY})"

# After 32 constants were compared one by one, the column's remaining constants are compared in one pass, which answers the later sets too.
add "k IN (1000..1031) OR k IN (1100..1131) OR k IN (1200..1231, 22)" \
    "k IN ($(seq -s ', ' 1000 1031)) OR k IN ($(seq -s ', ' 1100 1131)) OR k IN ($(seq -s ', ' 1200 1231), 22)"
# Which condition holds a found constant decides which row groups are read.
add "k IN (1000..1039, 2, 3, 31) AND k IN (1100..1139, 3, 12)" \
    "k IN ($(seq -s ', ' 1000 1039), 2, 3, 31) AND k IN ($(seq -s ', ' 1100 1139), 3, 12)"
add "k IN (1000..1039, 2) AND k IN (2, 13)" "k IN ($(seq -s ', ' 1000 1039), 2) AND k IN (2, 13)"

# Nulls read as the default value of a non-nullable column are not in the dictionary, but match `= 0`.
add "n64 = 0" "" "n64 Int64"
add "n64 IN (0, 1)" "" "n64 Int64"
add "n64 = 6" "" "n64 Int64"

# Every query runs without the dictionary filter, then with it (one client each); printed side by side.
ALL=$(printf '%s; ' "${QUERIES[@]}")
mapfile -t OFF < <(${CH} --input_format_parquet_dictionary_filter_push_down=0 --query="${ALL}" | jq -c '.data[0]["count()"]')
mapfile -t ON < <(${CH} --input_format_parquet_dictionary_filter_push_down=1048576 --query="${ALL}" \
    | jq -c '{count: .data[0]["count()"], rows_read: .statistics.rows_read}')
for i in "${!LABELS[@]}"; do
    echo "${LABELS[$i]}: off ${OFF[$i]}, on ${ON[$i]}"
done

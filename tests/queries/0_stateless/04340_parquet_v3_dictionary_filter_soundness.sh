#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Soundness regressions for dictionary-based row group filtering in the Parquet v3 reader. The
# dictionary filter must never change observable behavior compared to reading without it:
#  1. Reading nulls into a non-nullable column without `input_format_null_as_default` raises
#     `CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN`; the dictionary (which holds only non-null values) must
#     not skip such a row group and suppress that error.
#  2. `input_format_parquet_bloom_filter_push_down=0` must disable bloom filtering even when the
#     dictionary filter reused the per-column "we hashed the query constants" flag.

echo "1. the dictionary filter must not suppress the null-conversion error"

DATA1="${CLICKHOUSE_TEST_UNIQUE_NAME}_nulls.parquet"
# One row group of 100 rows: an optional (nullable) column, dictionary-encoded, ~a third NULL,
# non-null values 1000..1004. `max_block_size` is pinned so it is written as a single row group.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA1}', Parquet)
    select if(number % 3 = 0, NULL, toUInt64(1000 + number % 5)) as x
    from numbers(100)
    settings output_format_parquet_row_group_size = 100000, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# Read the optional column as non-nullable with null_as_default=0: decoding a null must raise an error.
# Disable the other filters so the dictionary filter is observed in isolation.
CH1="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0 --input_format_null_as_default=0"

# Prints the error code name if the query fails, nothing otherwise.
err() {
    ${CH1} --input_format_parquet_dictionary_filter_push_down="$1" --query="$2" 2>&1 \
        | grep -oE 'CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN' | head -1
}

echo "filter off: reading nulls as non-nullable raises an error (baseline)"
err 0 "select count() from file('${DATA1}', Parquet, 'x UInt64') where x = 9999"
echo "filter on: the row group must NOT be skipped, so the same error is raised (not suppressed)"
err 1048576 "select count() from file('${DATA1}', Parquet, 'x UInt64') where x = 9999"

# Reading the same column as Nullable is fine; nulls don't match equality, so pruning stays sound.
jrun1() {
    ${CH1} --input_format_parquet_dictionary_filter_push_down="$1" --query="$2 FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}
echo "filter on, read as Nullable: nulls stay null, so the row group is still pruned"
jrun1 1048576 "select count() from file('${DATA1}', Parquet, 'x Nullable(UInt64)') where x = 9999"

echo "2. input_format_parquet_bloom_filter_push_down=0 must disable bloom filtering"

DATA2="${CLICKHOUSE_TEST_UNIQUE_NAME}_bloom.parquet"
# Two row groups of 1000 rows, with bloom filters written for both. A tiny
# `output_format_parquet_max_dictionary_size` keeps the low-cardinality row group dictionary-encoded
# while forcing the high-cardinality one to fall back to PLAIN (its dictionary exceeds the limit), so
# exactly one row group is dictionary-filter eligible. Without this, 1000 short distinct strings still
# fit under the 1 MiB default dictionary size, so row group 1 would stay dictionary-encoded and be
# pruned by the dictionary filter too, defeating the bloom-only check below.
#  - row group 0: low cardinality ('lo_0'..'lo_3'), fully dictionary-encoded (dictionary-filter eligible)
#  - row group 1: all distinct ('hi_1000'..'hi_1999'), falls back to PLAIN (bloom-filter only)
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA2}', Parquet)
    select (number < 1000 ? 'lo_' || toString(number % 4) : 'hi_' || toString(number)) as s
    from numbers(2000)
    settings output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1, max_block_size = 1000000,
             output_format_parquet_write_bloom_filter = 1, output_format_parquet_max_dictionary_size = 1024;
"

CH2="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0"
jrun2() {
    ${CH2} --input_format_parquet_dictionary_filter_push_down="$1" --input_format_parquet_bloom_filter_push_down="$2" --query="$3 FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

echo "all filters off: both row groups are read"
jrun2 0 0 "select count() from file('${DATA2}', Parquet) where s = 'absent_value'"
echo "dict on, bloom on: row group 0 pruned by dictionary, row group 1 pruned by bloom"
jrun2 1048576 1 "select count() from file('${DATA2}', Parquet) where s = 'absent_value'"
echo "dict on, bloom off: row group 0 pruned by dictionary, row group 1 read (bloom is disabled)"
jrun2 1048576 0 "select count() from file('${DATA2}', Parquet) where s = 'absent_value'"
echo "dict on, bloom off, value only in row group 1: found, and row group 1 is read"
jrun2 1048576 0 "select count() from file('${DATA2}', Parquet) where s = 'hi_1500'"

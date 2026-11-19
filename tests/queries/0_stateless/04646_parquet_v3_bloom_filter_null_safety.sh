#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Soundness regression for bloom-filter row group pruning in the Parquet v3 reader with nullable
# columns read as non-nullable. The bloom filter is built only from the chunk's non-null values, so
# on a chunk that may contain nulls read into a non-nullable output it must not be used at all:
# - with `input_format_null_as_default = 0`, reading a null must raise
#   `CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN`; pruning the row group would suppress that exception;
# - with `input_format_null_as_default = 1`, nulls decode to the type's default value, which is not
#   in the bloom filter, so a row group whose only matches come from nulls would be wrongly skipped.
# This must hold both for the standalone bloom filter path (dictionary filter disabled or chunk not
# dictionary-eligible) and for the bloom filter used as the exact dictionary filter's fallback.

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# One row group of 100 rows: an optional (nullable) column with nulls, non-null values 1000..1004,
# a bloom filter written for the chunk. `max_block_size` / `output_format_parquet_row_group_size`
# are pinned so the data is written as a single row group deterministically.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select if(number % 3 = 0, NULL, toUInt64(1000 + number % 5)) as x
    from numbers(100)
    settings output_format_parquet_row_group_size = 100000, output_format_parquet_write_bloom_filter = 1,
             engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# Disable the min/max and page filters so pruning happens only via the bloom or dictionary filter.
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0 --input_format_parquet_bloom_filter_push_down=1"

# Prints the query result and the number of rows read from the file.
run() {
    local dict_limit="$1"
    local null_as_default="$2"
    local query="$3"
    ${CH} --input_format_parquet_dictionary_filter_push_down="${dict_limit}" --input_format_null_as_default="${null_as_default}" --query="${query} FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

echo "standalone bloom filter, null_as_default off: the row group must not be pruned, reading a null must throw"
${CH} --input_format_parquet_dictionary_filter_push_down=0 --input_format_null_as_default=0 \
    --query="select count() from file('${DATA_FILE}', Parquet, 'x UInt64') where x = 9999" 2>&1 \
    | grep -o "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN" | head -1

echo "standalone bloom filter, null_as_default on: nulls decode to the default 0, the row group must not be pruned"
run 0 1 "select count() from file('${DATA_FILE}', Parquet, 'x UInt64') where x = 0"

echo "dictionary filter declines (nullable, null_as_default off): the bloom fallback must not prune, reading a null must throw"
${CH} --input_format_parquet_dictionary_filter_push_down=1048576 --input_format_null_as_default=0 \
    --query="select count() from file('${DATA_FILE}', Parquet, 'x UInt64') where x = 9999" 2>&1 \
    | grep -o "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN" | head -1

echo "dictionary filter with null_as_default on: the default hash keeps the row group, count matches"
run 1048576 1 "select count() from file('${DATA_FILE}', Parquet, 'x UInt64') where x = 0"

echo "read as Nullable: nulls stay null, the bloom filter still prunes a non-present value"
run 0 0 "select count() from file('${DATA_FILE}', Parquet, 'x Nullable(UInt64)') where x = 9999"

# Control: a required (non-nullable) column is unaffected by the guard - the bloom filter still prunes.
DATA_FILE_2="${CLICKHOUSE_TEST_UNIQUE_NAME}_required.parquet"
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE_2}', Parquet)
    select toUInt64(1000 + number % 5) as x
    from numbers(100)
    settings output_format_parquet_row_group_size = 100000, output_format_parquet_write_bloom_filter = 1,
             engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"
echo "required column: the bloom filter prunes a non-present value"
${CH} --input_format_parquet_dictionary_filter_push_down=0 --input_format_null_as_default=0 --query="select count() from file('${DATA_FILE_2}', Parquet, 'x UInt64') where x = 9999 FORMAT JSON" \
    | jq -c '{result: .data, rows_read: .statistics.rows_read}'

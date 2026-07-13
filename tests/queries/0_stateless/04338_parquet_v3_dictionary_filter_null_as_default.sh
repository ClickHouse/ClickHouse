#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Soundness regression for dictionary-based row group filtering in the Parquet v3 reader.
# The dictionary page only holds the non-null values of a column chunk. Under
# `input_format_null_as_default`, null values read into a non-nullable column decode as the type's
# default value, which is not in the dictionary. The dictionary filter must account for that default
# value, otherwise it wrongly skips a row group whose nulls match the queried default.

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# One row group of 100 rows: an optional (nullable) column, dictionary-encoded, with nulls.
# Non-null values are 1000..1004; about a third of the rows are NULL. `max_block_size` is pinned so
# the data is written as a single row group deterministically.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select if(number % 3 = 0, NULL, toUInt64(1000 + number % 5)) as x
    from numbers(100)
    settings output_format_parquet_row_group_size = 100000, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# Disable the other row-group/page filters so we observe the dictionary filter in isolation, and read
# the optional column as non-nullable so nulls become the default value 0 (not present in the dictionary).
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0 --input_format_null_as_default=1"

run() {
    local limit="$1"
    local query="$2"
    ${CH} --input_format_parquet_dictionary_filter_push_down="${limit}" --query="${query} FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

echo "filter off: nulls read as default 0, count matches"
run 0 "select count() from file('${DATA_FILE}', Parquet, 'x UInt64') where x = 0"

echo "filter on: the row group must NOT be skipped, because its nulls decode to the queried default 0"
run 1048576 "select count() from file('${DATA_FILE}', Parquet, 'x UInt64') where x = 0"

echo "filter on: a value present in the dictionary is found"
run 1048576 "select count() from file('${DATA_FILE}', Parquet, 'x UInt64') where x = 1003"

echo "filter on: a value in neither the dictionary nor the default is pruned"
run 1048576 "select count() from file('${DATA_FILE}', Parquet, 'x UInt64') where x = 9999"

echo "filter on, read as Nullable: nulls stay null, so a non-present value is still pruned"
run 1048576 "select count() from file('${DATA_FILE}', Parquet, 'x Nullable(UInt64)') where x = 9999"

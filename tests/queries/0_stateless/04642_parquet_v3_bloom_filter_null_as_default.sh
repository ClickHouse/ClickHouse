#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Soundness regression for bloom-filter row group pruning in the Parquet v3 reader (the bloom analogue
# of 04338_parquet_v3_dictionary_filter_null_as_default). The parquet bloom filter covers only the
# non-null values of a column chunk, so when an optional column that may contain nulls is read as a
# non-nullable output column:
#  - under `input_format_null_as_default`, nulls decode as the type's default value, which the bloom
#    filter does not cover: a probe for the default must not prune the row group;
#  - without `input_format_null_as_default`, reading raises `CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN`;
#    pruning would suppress that error, so the bloom filter must not be used at all.
# This applies both to the plain bloom filter path and to the bloom fallback of the exact dictionary
# filter (taken when the dictionary does not fit the pruning memory budget).

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# One row group of 20000 rows: an optional (nullable) column with nulls, carrying both a (large)
# dictionary page and a bloom filter. The non-null values 1000..20999 are all distinct so the decoded
# dictionary (~160 KiB) exceeds the tight pruning watermark used below, forcing the dictionary filter
# to fall back to the bloom filter at runtime. `output_format_parquet_max_dictionary_size` is raised so
# the writer keeps the column dictionary-encoded, and `max_block_size` /
# `output_format_parquet_row_group_size` are pinned so the row group boundaries are deterministic
# regardless of the randomized settings in CI.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select if(number % 3 = 0, NULL, toUInt64(1000 + number)) as x
    from numbers(20000)
    settings output_format_parquet_row_group_size = 100000, output_format_parquet_max_dictionary_size = 100000000,
             output_format_parquet_write_bloom_filter = 1, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# Disable the min/max and page filters so pruning happens only via the bloom or dictionary filter.
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0 --input_format_parquet_bloom_filter_push_down=1"

# Prints the query result and the number of rows read from the file.
run() {
    local null_as_default="$1"
    local dictionary="$2"
    local watermark="$3"
    local query="$4"
    ${CH} --input_format_null_as_default="${null_as_default}" --input_format_parquet_dictionary_filter_push_down="${dictionary}" \
        --input_format_parquet_memory_high_watermark="${watermark}" --query="${query} FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

QUERY_DEFAULT="select count() from file('${DATA_FILE}', Parquet, 'x UInt64') where x = 0"
QUERY_ABSENT="select count() from file('${DATA_FILE}', Parquet, 'x UInt64') where x = 999999"

echo "plain bloom filter, null_as_default: the row group must NOT be pruned, its nulls decode to the queried default 0"
run 1 0 0 "${QUERY_DEFAULT}"

echo "plain bloom filter, null_as_default: a value in neither the data nor the default is pruned"
run 1 0 0 "${QUERY_ABSENT}"

echo "plain bloom filter, no null_as_default: pruning would suppress the null conversion error, so the row group is read and the error is raised"
${CH} --input_format_null_as_default=0 --input_format_parquet_dictionary_filter_push_down=0 --query="${QUERY_ABSENT}" 2>&1 \
    | grep -c "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN"

echo "plain bloom filter, read as Nullable: nulls stay null, so a non-present value is still pruned"
run 1 0 0 "select count() from file('${DATA_FILE}', Parquet, 'x Nullable(UInt64)') where x = 999999"

echo "bloom fallback of the dictionary filter (tight memory budget), null_as_default: the row group must NOT be pruned"
run 1 100000000 100000 "${QUERY_DEFAULT}"

echo "bloom fallback of the dictionary filter (tight memory budget), null_as_default: an absent value is still pruned"
run 1 100000000 100000 "${QUERY_ABSENT}"

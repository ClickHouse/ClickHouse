#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

# The dictionary-filter push-down bounds the memory it uses before decoding a dictionary page: it
# predicts the decoded footprint from the page header (`Dictionary::decodedFootprintUpperBound`) and
# skips pruning when it would not fit `input_format_parquet_memory_high_watermark`. For a fixed-size
# decoded type (here `Int32`) that footprint is `num_values * value_size` regardless of the page
# encoding, which is a conservative bound the check relies on. The other budget tests
# (04611/04612/04613) all use string dictionaries; this one exercises the fixed-size decode path and
# the shared value-set reservation for a non-string dictionary. It checks that the fixed-size
# dictionary filter prunes under a generous budget and stays correct (full-scan fallback) as the
# budget tightens.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 2 row groups of 20000 rows. Every row has a distinct Int32 `category`, so each row group's dictionary
# holds 20000 entries; the two value sets are disjoint (different row-group offset). The value 5 exists
# only in row group 0, so row group 1 is prunable. `output_format_parquet_max_dictionary_size` is raised
# so the writer keeps the column dictionary-encoded; `max_block_size` /
# `output_format_parquet_row_group_size` pin the row group boundaries deterministically.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select number as n, toInt32(intDiv(number, 20000) * 100000 + (number % 20000)) as category
    from numbers(40000)
    settings output_format_parquet_row_group_size = 20000, output_format_parquet_max_dictionary_size = 100000000, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# Isolate the dictionary filter and keep it always applicable.
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0 --input_format_parquet_dictionary_filter_push_down=100000000"

run() {
    local watermark="$1"
    local query="$2"
    ${CH} --input_format_parquet_memory_high_watermark="${watermark}" --query="${query} FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

echo "generous memory budget: the fixed-size dictionary filter prunes row group 1, only 20000 rows are read"
run 4000000000 "select count() from file('${DATA_FILE}', Parquet) where category = 5"

echo "tight memory budget below the decoded dictionary size: pruning is skipped, all 40000 rows are read"
run 100000 "select count() from file('${DATA_FILE}', Parquet) where category = 5"

echo "extreme memory budget (1 byte): pruning is skipped, result is still correct"
run 1 "select count() from file('${DATA_FILE}', Parquet) where category = 5"

echo "results are identical regardless of the memory budget, including across row groups"
diff \
    <(${CH} --input_format_parquet_memory_high_watermark=4000000000 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) where category in (5, 100007, 100, 119999) group by category order by category") \
    <(${CH} --input_format_parquet_memory_high_watermark=1 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) where category in (5, 100007, 100, 119999) group by category order by category") \
    && echo "OK"

#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The dictionary-filter pruning path must stay within `input_format_parquet_memory_high_watermark`
# even for its own bookkeeping, not just the raw dictionary page bytes. For a dictionary of many short
# strings the per-entry overhead dominates: `Dictionary::decode` allocates a `UInt32` offset per value
# on top of the decompressed page (`StringPlain` mode), and `hashDictionaryValues` then materializes a
# `ColumnString` whose `UInt64` offsets and geometrically grown `chars` buffer exceed the raw string
# bytes. The reader now predicts that full decoded footprint (in `Dictionary::decodedFootprintUpperBound`,
# checked before the page is decoded in `decodeDictionaryPage`) and estimates the materialized value
# set's footprint accordingly, so it never overshoots the budget even transiently. This test uses such
# a short-string dictionary to exercise those overheads and checks that pruning still works under a
# generous budget and that results stay correct as the budget tightens to a full-scan fallback.

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 2 row groups of 20000 rows. `category` values are short (3-7 chars) and distinct per row, disjoint
# between row groups (different row-group prefix), so each row group's dictionary holds 20000 entries
# whose ~80 KiB of UInt32 offsets are a large fraction of the decoded page - exactly the case where
# accounting only the page bytes would understate the footprint.
# `output_format_parquet_max_dictionary_size` is raised so the writer keeps the column dictionary-encoded;
# `max_block_size` / `output_format_parquet_row_group_size` pin the row group boundaries deterministically.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select number as n, concat(toString(intDiv(number, 20000)), '_', toString(number % 20000)) as category
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

# '0_5' exists only in row group 0, so row group 1 is prunable.
echo "generous memory budget: the dictionary filter prunes row group 1, only 20000 rows are read"
run 4000000000 "select count() from file('${DATA_FILE}', Parquet) where category = '0_5'"

echo "tight memory budget below the decoded dictionary footprint: pruning is skipped, all 40000 rows are read"
run 100000 "select count() from file('${DATA_FILE}', Parquet) where category = '0_5'"

echo "extreme memory budget (1 byte): pruning is skipped, result is still correct"
run 1 "select count() from file('${DATA_FILE}', Parquet) where category = '0_5'"

echo "results are identical regardless of the memory budget, including across row groups"
diff \
    <(${CH} --input_format_parquet_memory_high_watermark=4000000000 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) where category in ('0_5', '1_7', '0_100', '1_19999') group by category order by category") \
    <(${CH} --input_format_parquet_memory_high_watermark=1 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) where category in ('0_5', '1_7', '0_100', '1_19999') group by category order by category") \
    && echo "OK"

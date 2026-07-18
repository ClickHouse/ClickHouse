#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The dictionary-filter push-down is eligible based on the *compressed* on-disk dictionary page size
# (`input_format_parquet_dictionary_filter_push_down`), which does not bound the *decoded* dictionary.
# A large (or highly compressible) dictionary can therefore decode to far more than that limit. To keep
# the default-on optimization within `input_format_parquet_memory_high_watermark`, both the decoded
# dictionary page (before it is decompressed, in `decodeDictionaryPage`) and the decoded value set
# built for hashing (in `hashDictionaryValues`) are capped against that watermark; when the dictionary
# would exceed it, pruning is skipped and the reader falls back to a full scan. This test checks that
# the fallback keeps results correct and disables pruning under a tight memory budget.

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 2 row groups of 20000 rows. Every row has a distinct `category` value, so each row group's dictionary
# holds 20000 entries; the two value sets are disjoint (different row-group prefix). The decoded
# dictionary of each row group is ~260 KiB, far above the tight watermark used below but eligible for
# the generous `dictionary_filter_limit_bytes` we pass. `output_format_parquet_max_dictionary_size` is
# raised so the writer keeps the (large) column dictionary-encoded instead of falling back to PLAIN, and
# `max_block_size` / `output_format_parquet_row_group_size` are pinned so the row group boundaries are
# deterministic regardless of the randomized settings in CI.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select number as n, concat('rg', toString(intDiv(number, 20000)), '_val_', toString(number % 20000)) as category
    from numbers(40000)
    settings output_format_parquet_row_group_size = 20000, output_format_parquet_max_dictionary_size = 100000000, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# Disable the other row-group/page filters so we observe the dictionary filter in isolation, and keep a
# generous dictionary-page eligibility limit so the dictionary filter is always applicable.
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0 --input_format_parquet_dictionary_filter_push_down=100000000"

# Prints the query result and the number of rows read from the file.
run() {
    local watermark="$1"
    local query="$2"
    ${CH} --input_format_parquet_memory_high_watermark="${watermark}" --query="${query} FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

# 'rg0_val_5' exists only in row group 0, so row group 1 is prunable.
echo "generous memory budget: the dictionary filter prunes row group 1, only 20000 rows are read"
run 4000000000 "select count() from file('${DATA_FILE}', Parquet) where category = 'rg0_val_5'"

echo "tight memory budget below the decoded dictionary size: pruning is skipped, all 40000 rows are read"
run 100000 "select count() from file('${DATA_FILE}', Parquet) where category = 'rg0_val_5'"

echo "extreme memory budget (1 byte): pruning is skipped, result is still correct"
run 1 "select count() from file('${DATA_FILE}', Parquet) where category = 'rg0_val_5'"

echo "results are identical regardless of the memory budget, including across row groups"
diff \
    <(${CH} --input_format_parquet_memory_high_watermark=4000000000 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) where category in ('rg0_val_5', 'rg1_val_7', 'rg0_val_100', 'rg1_val_19999') group by category order by category") \
    <(${CH} --input_format_parquet_memory_high_watermark=100000 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) where category in ('rg0_val_5', 'rg1_val_7', 'rg0_val_100', 'rg1_val_19999') group by category order by category") \
    && echo "OK"

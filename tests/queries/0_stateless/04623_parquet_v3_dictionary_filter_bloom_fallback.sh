#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: when the exact dictionary filter declines at runtime because its decoded dictionary
# (or value set) does not fit the pruning memory budget, a column chunk that also carries a bloom filter
# must fall back to the bloom filter instead of reading the whole row group. Without this fallback the
# row group is read in full even though its bloom filter could rule it out - a regression from the
# pre-existing bloom-only behavior on files that carry both structures.

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 2 row groups of 20000 rows. Every row has a distinct `category` value, so each row group's dictionary
# holds 20000 entries; the two value sets are disjoint (different row-group prefix). The decoded
# dictionary of each row group is ~260 KiB, above the tight watermark used below but eligible for the
# generous `dictionary_filter_limit_bytes` we pass. A bloom filter is written for every column chunk,
# so `category` has both a dictionary page and a bloom filter. `output_format_parquet_max_dictionary_size`
# is raised so the writer keeps the (large) column dictionary-encoded instead of falling back to PLAIN,
# and `max_block_size` / `output_format_parquet_row_group_size` are pinned so the row group boundaries
# are deterministic regardless of the randomized settings in CI.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select number as n, concat('rg', toString(intDiv(number, 20000)), '_val_', toString(number % 20000)) as category
    from numbers(40000)
    settings output_format_parquet_row_group_size = 20000, output_format_parquet_max_dictionary_size = 100000000,
             output_format_parquet_write_bloom_filter = 1, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# Disable the min/max and page filters so pruning happens only via the dictionary or bloom filter, and
# keep a generous dictionary-page eligibility limit so the dictionary filter is always applicable.
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0 --input_format_parquet_dictionary_filter_push_down=100000000"

# Prints the query result and the number of rows read from the file.
run() {
    local watermark="$1"
    local bloom="$2"
    local query="$3"
    ${CH} --input_format_parquet_memory_high_watermark="${watermark}" --input_format_parquet_bloom_filter_push_down="${bloom}" --query="${query} FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

# 'rg0_val_5' exists only in row group 0, so row group 1 is prunable by either the dictionary or the
# bloom filter of `category`.
QUERY="select count() from file('${DATA_FILE}', Parquet) where category = 'rg0_val_5'"

echo "generous memory budget: the dictionary filter prunes row group 1, only 20000 rows are read"
run 4000000000 0 "${QUERY}"

echo "tight memory budget, bloom filter off: the dictionary can't fit and there is no fallback, all 40000 rows are read"
run 100000 0 "${QUERY}"

echo "tight memory budget, bloom filter on: the dictionary can't fit but the bloom filter falls back and prunes row group 1, only 20000 rows are read"
run 100000 1 "${QUERY}"

echo "results are identical regardless of the memory budget and bloom filter, including across row groups"
diff \
    <(${CH} --input_format_parquet_memory_high_watermark=4000000000 --input_format_parquet_bloom_filter_push_down=0 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) where category in ('rg0_val_5', 'rg1_val_7', 'rg0_val_100', 'rg1_val_19999') group by category order by category") \
    <(${CH} --input_format_parquet_memory_high_watermark=100000 --input_format_parquet_bloom_filter_push_down=1 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) where category in ('rg0_val_5', 'rg1_val_7', 'rg0_val_100', 'rg1_val_19999') group by category order by category") \
    && echo "OK"

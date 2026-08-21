#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

# The value-set reservation in `hashDictionaryValues` must be a genuine upper bound on what the
# pruning path really allocates. Its materialized-payload term must be derived from the exact total
# value bytes of the dictionary, not from `getAverageValueSize`: flooring the fractional mean and
# multiplying back by `count` understates a mixed-length string dictionary by up to `count` bytes
# (twice that in the reservation, which doubles the payload to cover geometric `chars` growth).
# This test builds a dictionary of 19999 three-char strings plus one two-char string per row group,
# so the mean value size is 2.99995: the floored estimate would charge `2 * 2 * 20000` bytes for the
# materialized `chars` while the exact one charges `2 * 59999`. The watermark is pinned inside that
# gap (the absolute value also covers the hash vector and the per-value materialization terms of the
# reservation): with the exact reservation the value set does not fit and the filter falls back to a
# full scan; a regression to the floored estimate would consider it affordable and prune again,
# flipping `rows_read` and failing this test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 2 row groups of 20000 rows, all `category` values distinct and disjoint between row groups:
# per row group, 19999 distinct three-char strings and one two-char string ('!a' / '!b').
# `output_format_parquet_max_dictionary_size` is raised so the writer keeps the column
# dictionary-encoded; `max_block_size` / `output_format_parquet_row_group_size` pin the row group
# boundaries deterministically.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select number as n,
      if(number % 20000 = 0,
         concat('!', substring('ab', 1 + toUInt32(intDiv(number, 20000)), 1)),
         concat(
           substring('abcdefghijklmnopqrstuvwxyz0123456789', 1 + toUInt32(intDiv(number, 1296) % 36), 1),
           substring('abcdefghijklmnopqrstuvwxyz0123456789', 1 + toUInt32(intDiv(number, 36) % 36), 1),
           substring('abcdefghijklmnopqrstuvwxyz0123456789', 1 + toUInt32(number % 36), 1))) as category
    from numbers(40000)
    settings output_format_parquet_row_group_size = 20000, output_format_parquet_max_dictionary_size = 100000000, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# Isolate the dictionary filter and keep it always applicable. The watermark is budgeted per reader
# stream, so pin the parsing parallelism to keep the per-reader share deterministic.
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0 --input_format_parquet_dictionary_filter_push_down=100000000 --max_threads=1 --max_parsing_threads=1"

run() {
    local watermark="$1"
    local query="$2"
    ${CH} --input_format_parquet_memory_high_watermark="${watermark}" --query="${query} FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

# '!a' exists only in row group 0, so row group 1 is prunable.
echo "generous memory budget: the dictionary filter prunes row group 1, only 20000 rows are read"
run 4000000000 "select count() from file('${DATA_FILE}', Parquet) where category = '!a'"

echo "watermark between the floored and the exact value-set footprint: pruning must be skipped, all 40000 rows are read"
run 6615000 "select count() from file('${DATA_FILE}', Parquet) where category = '!a'"

echo "extreme memory budget (1 byte): pruning is skipped, result is still correct"
run 1 "select count() from file('${DATA_FILE}', Parquet) where category = '!a'"

echo "results are identical regardless of the memory budget, including across row groups"
diff \
    <(${CH} --input_format_parquet_memory_high_watermark=4000000000 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) where category in ('!a', '!b', 'aaf', '45d') group by category order by category") \
    <(${CH} --input_format_parquet_memory_high_watermark=6615000 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) where category in ('!a', '!b', 'aaf', '45d') group by category order by category") \
    && echo "OK"

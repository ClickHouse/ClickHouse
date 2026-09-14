#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: dictionary-based row group filtering must apply to large IN lists.
#
# The dictionary filter reuses the bloom filter's prepared query-constant hashes. The bloom filter
# skips hashing an IN list with more than `bloom_filter_max_set_size` (100) elements, because a large
# probabilistic lookup rarely prunes and would read many filter blocks for nothing. The dictionary
# lookup is exact and reads no extra data per value, so that cap must not disable it: an IN list with
# more than 100 elements must still skip whole row groups whose dictionary proves the values absent.
# Before the fix, such lists silently fell back to a full row-group scan.

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 2 row groups of 10000 rows. `category` has 150 disjoint dictionary-encoded values per row group:
#  - row group 0: '0' .. '149'
#  - row group 1: '1000' .. '1149'
# 150 > bloom_filter_max_set_size (100), so an IN list covering a whole row group's value set
# exercises the path that used to disable the filter.
# `max_block_size` is pinned so the writer splits the data into deterministic 10000-row row groups
# (mirroring 04326_parquet_v3_dictionary_filter_push_down); otherwise the randomized `max_block_size`
# shifts the row group boundaries and the per-row-group value sets stop being disjoint.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select number as n, toString(intDiv(number, 10000) * 1000 + (number % 150)) as category
    from numbers(20000)
    settings output_format_parquet_row_group_size = 10000, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# Disable the other row-group/page filters so we observe the dictionary filter in isolation.
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0"

# 150-element IN lists, above the 100-element bloom filter set-size cap.
IN_RG0=$(seq 0 149 | sed "s/.*/'&'/" | paste -sd,)
IN_NONE=$(seq 900000 900149 | sed "s/.*/'&'/" | paste -sd,)

# Prints the query result and the number of rows read from the file.
run() {
    local limit="$1"
    local query="$2"
    ${CH} --input_format_parquet_dictionary_filter_push_down="${limit}" --query="${query} FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

echo "150-element IN, all values in row group 0, filter off: all 20000 rows are read"
run 0 "select count() from file('${DATA_FILE}', Parquet) where category in (${IN_RG0})"

echo "150-element IN, all values in row group 0, filter on: only row group 0 (10000 rows) is read"
run 1048576 "select count() from file('${DATA_FILE}', Parquet) where category in (${IN_RG0})"

echo "150-element IN, no value present in any row group, filter on: all row groups are skipped"
run 1048576 "select count() from file('${DATA_FILE}', Parquet) where category in (${IN_NONE})"

echo "the 150-element IN produces the same result with and without the filter"
diff \
    <(${CH} --input_format_parquet_dictionary_filter_push_down=0 --query="select count() from file('${DATA_FILE}', Parquet) where category in (${IN_RG0})") \
    <(${CH} --input_format_parquet_dictionary_filter_push_down=1048576 --query="select count() from file('${DATA_FILE}', Parquet) where category in (${IN_RG0})") \
    && echo "OK"

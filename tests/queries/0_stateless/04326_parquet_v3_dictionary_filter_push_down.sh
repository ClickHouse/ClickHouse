#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Dictionary-based row group filtering for the Parquet v3 reader: when all data pages of a column
# chunk are dictionary-encoded, the dictionary page holds the complete set of values, so whole row
# groups can be skipped for equality/IN conditions.

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 3 row groups of 10000 rows. The `category` column has disjoint value sets per row group:
#  - row group 0: '0' .. '49'
#  - row group 1: '1000' .. '1049'
#  - row group 2: '2000' .. '2049'
# Each value set is tiny, so the writer dictionary-encodes the column.
# `max_block_size` is pinned so the data arrives in a single block and the writer splits it into row
# groups deterministically; otherwise the randomized `max_block_size` shifts the row group
# boundaries, the per-row-group value sets stop being disjoint, and the expected row counts change.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select number as n, toString(intDiv(number, 10000) * 1000 + (number % 50)) as category
    from numbers(30000)
    settings output_format_parquet_row_group_size = 10000, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# Disable the other row-group/page filters so we observe the dictionary filter in isolation.
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0"

# Prints the query result and the number of rows read from the file.
run() {
    local limit="$1"
    local query="$2"
    ${CH} --input_format_parquet_dictionary_filter_push_down="${limit}" --query="${query} FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

echo "filter off: all 30000 rows are read"
run 0 "select count() from file('${DATA_FILE}', Parquet) where category = '1005'"

echo "filter on: only the matching row group (10000 rows) is read"
run 1048576 "select count() from file('${DATA_FILE}', Parquet) where category = '1005'"

echo "filter on, value present in no row group: all row groups are skipped"
run 1048576 "select count() from file('${DATA_FILE}', Parquet) where category = 'no_such_value'"

echo "filter on, IN with values in row groups 0 and 2: row group 1 is skipped"
run 1048576 "select count() from file('${DATA_FILE}', Parquet) where category in ('5', '2005')"

echo "filter on, but dictionary page is larger than the limit: filter is disabled"
run 100 "select count() from file('${DATA_FILE}', Parquet) where category = '1005'"

echo "results are identical regardless of the filter, across all values"
diff \
    <(${CH} --input_format_parquet_dictionary_filter_push_down=0 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) group by category order by category") \
    <(${CH} --input_format_parquet_dictionary_filter_push_down=1048576 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) group by category order by category") \
    && echo "OK"

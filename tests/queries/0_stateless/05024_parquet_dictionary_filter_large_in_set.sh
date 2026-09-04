#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

# `IN` sets larger than the bloom filter's set-size cap (`bloom_filter_max_set_size`, 100) are hashed
# only for the exact dictionary filter of the native Parquet reader, so `findAnyHash` gets thousands of
# probe hashes for one column chunk. The dictionary lookup intersects them with its own sorted vector
# of value hashes in one pass over both sequences, which only works if it is oblivious to how the
# probes and the values interleave. This test checks the pruning decisions and the results of that
# intersection: a large set that misses every row group, one that hits a single row group, and one
# that hits all of them, each compared against the same query with the dictionary filter disabled.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 4 row groups of 8192 distinct Int32 values each: row group `g` holds `g * 100000 + [0, 8192)`.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select number as n, toInt32(intDiv(number, 8192) * 100000 + (number % 8192)) as category
    from numbers(32768)
    settings output_format_parquet_row_group_size = 8192, output_format_parquet_max_dictionary_size = 100000000, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0 --max_threads=1 --max_parsing_threads=1"

# `$1` is the `IN` set expression: the same query runs with and without the dictionary filter.
check() {
    local set_expression="$1"
    local query="select count(), sum(n), min(category), max(category) from file('${DATA_FILE}', Parquet) where category in (${set_expression})"
    local with_filter
    local without_filter
    # The `numbers(5000)` of the set subquery is counted in `rows_read` too, so subtract it to leave
    # the number of rows read from the Parquet file (32768 in total, in 4 row groups of 8192).
    with_filter=$(${CH} --input_format_parquet_dictionary_filter_push_down=100000000 --query="${query} FORMAT JSON" | jq -c '{result: .data, parquet_rows_read: (.statistics.rows_read - 5000)}')
    without_filter=$(${CH} --input_format_parquet_dictionary_filter_push_down=0 --query="${query} FORMAT JSON" | jq -c '.data')
    echo "${with_filter}"
    if [ "$(echo "${with_filter}" | jq -c '.result')" = "${without_filter}" ]; then
        echo "same result without the dictionary filter"
    else
        echo "MISMATCH: ${without_filter}"
    fi
}

echo "a set of 5000 values that no row group holds: every row group is pruned"
check "select 1000000 + number * 3 from numbers(5000)"

echo "a set of 5000 values of which one is in row group 2 only: only that row group is read"
check "select if(number = 4000, 200077, 1000000 + number * 3) from numbers(5000)"

echo "a set of 5000 values interleaved with every row group: nothing is pruned"
check "select intDiv(number, 1250) * 100000 + (number % 1250) * 2 from numbers(5000)"

echo "a set of 5000 values matching only the last value of the last row group"
check "select if(number = 4999, 308191, 1000000 + number * 3) from numbers(5000)"

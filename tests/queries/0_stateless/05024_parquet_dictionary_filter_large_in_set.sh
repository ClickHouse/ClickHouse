#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.
# Random settings limits: max_bytes_before_external_set=(0, 0); max_bytes_ratio_before_external_set=(0, 0)
# A set of `IN` spilled to disk keeps no elements, so the dictionary filter could not prune with it.

# `IN` sets larger than the bloom filter's set-size cap (`bloom_filter_max_set_size`, 100) are hashed
# only for the exact dictionary filter of the native Parquet reader, so `findAnyHash` gets thousands of
# probe hashes for one column chunk. Each set here is the only condition on its column, so the lookup
# searches it for each dictionary value (sets that share a column with another condition are in 05317).
# This test checks the pruning decisions and the results of that search: a large set that misses every
# row group, one that hits a single row group, and one that hits all of them, each compared against the
# same query with the dictionary filter disabled. The two files read together hash the same set as Int32
# and as Int64, which must not share the hashes of the other file.

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
# `$2` is the size of the set (5000 by default), `$3` the table function (the file above by default)
# and `$4` a SETTINGS clause.
check() {
    local set_expression="$1"
    local set_size="${2:-5000}"
    local source="$3"
    [ -n "${source}" ] || source="file('${DATA_FILE}', Parquet)"
    local query="select count(), sum(n), min(category), max(category) from ${source} where category in (${set_expression}) $4"
    local with_filter
    local without_filter
    # The `numbers(...)` of the set subquery is counted in `rows_read` too, so subtract it to leave
    # the number of rows read from the Parquet file (32768 in total, in 4 row groups of 8192).
    with_filter=$(${CH} --input_format_parquet_dictionary_filter_push_down=100000000 --query="${query} FORMAT JSON" | jq -c "{result: .data, parquet_rows_read: (.statistics.rows_read - ${set_size})}")
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

echo "a set of 20000 values that no row group holds: every row group is pruned"
check "select 1000000 + number * 3 from numbers(20000)" 20000

echo "a set of 20000 values of which one is in row group 2 only: only that row group is read"
check "select if(number = 16000, 200077, 1000000 + number * 3) from numbers(20000)" 20000

echo "a set of 20000 values matching only the last value of the last row group"
check "select if(number = 19999, 308191, 1000000 + number * 3) from numbers(20000)" 20000

# The same 4 row groups, written as Int32 into one file and as Int64 into the other.
for type in Int32 Int64; do
    ${CLICKHOUSE_CLIENT} --query="
        insert into function file('${CLICKHOUSE_TEST_UNIQUE_NAME}_${type}.parquet', Parquet)
        select number as n, to${type}(intDiv(number, 8192) * 100000 + (number % 8192)) as category
        from numbers(32768)
        settings output_format_parquet_row_group_size = 8192, output_format_parquet_max_dictionary_size = 100000000, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
    "
done
TWO_FILES="file('${CLICKHOUSE_TEST_UNIQUE_NAME}_Int{32,64}.parquet', Parquet, 'n UInt64, category Int64')"

echo "an Int32 and an Int64 file with one value of a set of 5000 in row group 2: that row group of each file is read"
check "select if(number = 4000, 200077, 1000000 + number * 3) from numbers(5000)" 5000 "${TWO_FILES}"
check "select if(number = 4000, 200077, 1000000 + number * 3) from numbers(5000)" 5000 "${TWO_FILES}" "settings max_threads = 2"

# Row group 1 has a null in every 16th row, read as the default 0 of a non-Nullable column; 0 is in no dictionary.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${CLICKHOUSE_TEST_UNIQUE_NAME}_nullable.parquet', Parquet)
    select number as n, if(intDiv(number, 8192) = 1 and number % 16 = 0, null, toInt32(intDiv(number, 8192) * 100000 + 1 + (number % 8192)))::Nullable(Int32) as category
    from numbers(32768)
    settings output_format_parquet_row_group_size = 8192, output_format_parquet_max_dictionary_size = 100000000, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

echo "a set of 20000 values holding only the default value: only the row group with nulls is read"
check "select if(number = 0, 0, 1000000 + number * 3) from numbers(20000)" 20000 "file('${CLICKHOUSE_TEST_UNIQUE_NAME}_nullable.parquet', Parquet, 'n UInt64, category Int32')"

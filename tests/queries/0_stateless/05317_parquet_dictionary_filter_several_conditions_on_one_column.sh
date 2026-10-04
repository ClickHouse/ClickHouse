#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

# Row group pruning by the Parquet dictionary filter when the WHERE clause has one or many conditions
# on the same column: every condition checks the dictionary of the same row group again, so a row
# group's dictionary can be checked up to 20 times, with the matching value in an early or in a late
# condition. Each query is compared with the same query with the dictionary filter disabled, and
# `rows_read` shows which row groups were pruned.

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

CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --query_plan_optimize_prewhere=0 --use_cache_for_count_from_files=0 --max_threads=1 --max_parsing_threads=1"

# Comma-separated values `1000000 + [$1, $2]`, which no row group holds.
absent() {
    seq -s ', ' $((1000000 + $1)) $((1000000 + $2))
}

# `category = v1 or category = v2 or ...` for the given values.
equalities() {
    local condition=""
    local value
    for value in "$@"; do
        condition="${condition:+${condition} or }category = ${value}"
    done
    echo "${condition}"
}

# `$1` is the condition: the same query runs with and without the dictionary filter. Equality chains
# must not be merged into one `IN`, so that every equality checks the dictionary on its own.
check() {
    local query="select count(), sum(n) from file('${DATA_FILE}', Parquet) where $1 settings optimize_min_equality_disjunction_chain_length = 1000"
    local with_filter
    local without_filter
    with_filter=$(${CH} --input_format_parquet_dictionary_filter_push_down=100000000 --query="${query} FORMAT JSON" | jq -c '{result: .data, rows_read: .statistics.rows_read}')
    without_filter=$(${CH} --input_format_parquet_dictionary_filter_push_down=0 --query="${query} FORMAT JSON" | jq -c '.data')
    echo "${with_filter}"
    if [ "$(echo "${with_filter}" | jq -c '.result')" = "${without_filter}" ]; then
        echo "same result without the dictionary filter"
    else
        echo "MISMATCH: ${without_filter}"
    fi
}

echo "one equality on a value of row group 1: only that row group is read"
check "category = 100005"

echo "one equality on a value that no row group holds: every row group is pruned"
check "category = 1000001"

echo "two IN sets of 10 values, the second one holding a value of row group 2: only that row group is read"
check "category in ($(absent 1 10)) or category in ($(absent 11 19), 200077)"

echo "two IN sets of 10 values, the first one holding a value of row group 3: only that row group is read"
check "category in ($(absent 1 9), 300123) or category in ($(absent 11 20))"

echo "20 equalities, the last one on a value of row group 0: only that row group is read"
check "$(equalities $(seq 1000001 1000019) 8191)"

echo "20 equalities on values that no row group holds: every row group is pruned"
check "$(equalities $(seq 1000001 1000020))"

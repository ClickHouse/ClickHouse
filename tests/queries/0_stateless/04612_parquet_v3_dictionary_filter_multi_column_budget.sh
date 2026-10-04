#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

# The dictionary-filter push-down evaluates all predicate columns of a row group in one
# `applyBloomAndDictionaryFilters` call. Each dictionary-filtered column builds a value set (a
# `HashSet` of the dictionary value hashes) that stays alive until that whole evaluation finishes, so
# a predicate over several dictionary-filtered columns must not let each column use the full
# `input_format_parquet_memory_high_watermark` independently and collectively overshoot it. The
# budget is shared across the columns' value sets. This test checks that filtering over two
# dictionary-encoded columns stays correct and effective under a generous budget, and stays correct
# (falling back to a full scan) as the shared budget tightens.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 2 row groups of 20000 rows, two dictionary-encoded string columns whose values are distinct per row
# and disjoint between row groups (different row-group prefix). Each row group's dictionary of each
# column holds 20000 entries (~260 KiB decoded). `output_format_parquet_max_dictionary_size` is raised
# so the writer keeps both columns dictionary-encoded; `max_block_size` /
# `output_format_parquet_row_group_size` pin the row group boundaries deterministically.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select number as n,
           concat('a_rg', toString(intDiv(number, 20000)), '_', toString(number % 20000)) as cat_a,
           concat('b_rg', toString(intDiv(number, 20000)), '_', toString(number % 20000)) as cat_b
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

# Both matched values live only in row group 0, so row group 1 is prunable when either column's
# dictionary filter is used.
Q="select count() from file('${DATA_FILE}', Parquet) where cat_a = 'a_rg0_5' or cat_b = 'b_rg0_9'"

echo "generous budget: both columns' value sets fit, row group 1 is pruned, only 20000 rows are read"
run 4000000000 "${Q}"

echo "tight budget shared across both columns: pruning is skipped, all 40000 rows are read"
run 100000 "${Q}"

echo "extreme budget (1 byte): pruning is skipped, result is still correct"
run 1 "${Q}"

echo "results over two dictionary-filtered columns are identical regardless of the shared budget"
diff \
    <(${CH} --input_format_parquet_memory_high_watermark=4000000000 --query="select count(), sum(n) from file('${DATA_FILE}', Parquet) where cat_a in ('a_rg0_5', 'a_rg1_7') or cat_b in ('b_rg0_100', 'b_rg1_19999')") \
    <(${CH} --input_format_parquet_memory_high_watermark=100000 --query="select count(), sum(n) from file('${DATA_FILE}', Parquet) where cat_a in ('a_rg0_5', 'a_rg1_7') or cat_b in ('b_rg0_100', 'b_rg1_19999')") \
    && echo "OK"

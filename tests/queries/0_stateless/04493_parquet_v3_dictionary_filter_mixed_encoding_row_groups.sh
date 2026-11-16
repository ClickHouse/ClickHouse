#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: dictionary filtering must be enabled per row group, not decided from the first
# surviving row group only. Encoding is a per-row-group property, so a file whose first row group
# falls back to PLAIN (high cardinality, no dictionary, and - since the writer does not write bloom
# filters by default - no bloom filter either) while a later row group stays dictionary-encoded must
# still prune the later row group. Previously prepareBloomFilterCondition looked only at row_groups[0]
# and, finding it ineligible, never hashed the query constants, so the dictionary-encoded later row
# group silently lost pruning.

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 2 row groups of 10000 rows:
#  - row group 0: 10000 distinct 'hi_<n>' values. Its dictionary exceeds the tiny
#    output_format_parquet_max_dictionary_size, so the writer falls back to PLAIN (no dictionary, and
#    no bloom filter is written by default) - this row group is not dictionary-filter eligible.
#  - row group 1: 50 distinct 'lo_<n>' values. Its dictionary is tiny, so it stays dictionary-encoded
#    and is dictionary-filter eligible.
# max_block_size is pinned so the data arrives in a single block and the writer splits it into row
# groups deterministically at the 10000-row boundary.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select number as n, if(number < 10000, 'hi_' || toString(number), 'lo_' || toString(number % 50)) as category
    from numbers(20000)
    settings output_format_parquet_row_group_size = 10000, output_format_parquet_max_dictionary_size = 1024,
             engine_file_truncate_on_insert = 1, max_block_size = 1000000;
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

# 'hi_5' lives only in the PLAIN row group 0; it is absent from the dictionary-encoded row group 1.
echo "filter off: both row groups are read (20000)"
run 0 "select count() from file('${DATA_FILE}', Parquet) where category = 'hi_5'"

echo "filter on: row group 1 (dictionary-encoded) is pruned even though row group 0 is PLAIN and ineligible (10000)"
run 1048576 "select count() from file('${DATA_FILE}', Parquet) where category = 'hi_5'"

echo "results are identical regardless of the filter, across all values"
diff \
    <(${CH} --input_format_parquet_dictionary_filter_push_down=0 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) group by category order by category") \
    <(${CH} --input_format_parquet_dictionary_filter_push_down=1048576 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) group by category order by category") \
    && echo "OK"

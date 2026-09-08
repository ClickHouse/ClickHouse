#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: a larger-than-cap `IN` set must not disable the bloom filter for smaller predicates
# on the same column.
#
# `IN` sets larger than `bloom_filter_max_set_size` (100) are hashed only for the exact dictionary
# filter (which reads no extra data per value); they are deliberately kept out of the bloom filter,
# which would otherwise read one filter block per value for little benefit. This must be tracked per
# predicate atom, not per column: a query like `x = 'a' OR (x IN (<large set>) AND b = 2)` has both a
# small hashable atom and an over-cap `IN` on `x`. The small `x = 'a'` atom must still prune a row
# group that is not dictionary-encoded (PLAIN) but has a bloom filter, using its one-hash bloom probe.
# A previous fix tracked the over-cap state per column, so the large `IN` disabled the bloom filter for
# the whole column, and the small atom fell back to a full row-group scan.

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 2 row groups of 10000 rows, with a bloom filter written for every column chunk:
#  - row group 0: 10000 distinct 'k00000'..'k09999' values. Its dictionary exceeds the tiny
#    output_format_parquet_max_dictionary_size, so it falls back to PLAIN encoding (not
#    dictionary-filter eligible) but still has a bloom filter. `b` is 0.
#  - row group 1: 50 distinct 'k00000'..'k00049' values (inside row group 0's min/max range), so it
#    stays dictionary-encoded and is dictionary-filter eligible. `b` is 2.
# max_block_size is pinned so the writer splits the data into deterministic 10000-row row groups.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select number as n,
           'k' || leftPad(toString(if(number < 10000, number, number % 50)), 5, '0') as x,
           if(number < 10000, toUInt32(0), toUInt32(2)) as b
    from numbers(20000)
    settings output_format_parquet_row_group_size = 10000, output_format_parquet_max_dictionary_size = 1024,
             output_format_parquet_write_bloom_filter = 1, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# min/max statistics prune 'b = 2' in row group 0; the dictionary filter is available for row group 1.
# The page filter is disabled so we observe pruning at row-group granularity.
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=1 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_dictionary_filter_push_down=1048576 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0"

# Prints the query result and the number of rows read from the file.
run() {
    local bloom="$1"
    local query="$2"
    ${CH} --input_format_parquet_bloom_filter_push_down="${bloom}" --query="${query} FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

# 'k00abs' is absent from the file but inside row group 0's min/max range, so only the bloom filter can
# rule it out there. The 150-element IN list (> 100) is over the bloom filter set-size cap and is used
# only by the dictionary filter; its values overlap both row groups' ranges.
IN_SET=$(seq 0 149 | xargs -I{} printf "k%05d\n" {} | sed "s/.*/'&'/" | paste -sd,)
QUERY="select count() from file('${DATA_FILE}', Parquet) where x = 'k00abs' or (x in (${IN_SET}) and b = 2)"

echo "bloom filter off: row group 0 (PLAIN) cannot be ruled out, both row groups are read (20000)"
run 0 "${QUERY}"

echo "bloom filter on: the small equality still prunes the PLAIN+bloom row group 0, even though an over-cap IN on the same column is used only by the dictionary filter (10000)"
run 1 "${QUERY}"

echo "the result is identical regardless of the bloom filter"
diff \
    <(${CH} --input_format_parquet_bloom_filter_push_down=0 --query="${QUERY}") \
    <(${CH} --input_format_parquet_bloom_filter_push_down=1 --query="${QUERY}") \
    && echo "OK"

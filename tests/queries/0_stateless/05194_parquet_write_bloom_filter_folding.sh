#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Parquet writer sizes the bloom filter of a column chunk for the number of values in the chunk
# (assuming every value is distinct) and then folds the filter down - merging neighbouring blocks -
# as long as the measured fill rate keeps the false positive probability under the one implied by
# `output_format_parquet_bloom_filter_bits_per_value`. This checks the two properties that folding
# must preserve:
#   * a chunk with few distinct values still gets a small filter, without deduplicating the hashes
#     in a hash set first;
#   * folding never drops a value, so a row group holding a matching value is never pruned.
#
# 100 row groups of 2000 rows. `u` is distinct in every row, so its filter is never folded and serves
# as the reference size. `c` takes only 4 distinct values per row group, so its filter folds all the
# way down to a single block, and every value of `c` belongs to exactly one row group, so a probe for
# it must read exactly that row group: 500 matching rows out of the 2000 read.
#
# The filter sizes are checked for every row group, but only every ${PROBE_STEP}-th row group is probed:
# each probe is a separate query, and a query over the file takes about a second on a loaded CI machine.
ROW_GROUPS=100
ROWS_PER_GROUP=2000
PROBE_STEP=10
PROBES=$((ROW_GROUPS / PROBE_STEP))

# One thread and one big block, so the row groups have exactly ${ROWS_PER_GROUP} rows each.
CH="${CLICKHOUSE_CLIENT} --output_format_parquet_row_group_size=${ROWS_PER_GROUP} --max_block_size=1000000 --max_threads=1 --max_insert_threads=1 --output_format_parquet_write_bloom_filter=1 --engine_file_truncate_on_insert=1"

# Disable every other pruning method so only the bloom filter decides which row groups are read.
READ="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_dictionary_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=1 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0"

function write_file()
{
    local file=$1
    local bits_per_value=$2
    ${CH} --query="
        insert into function file('${file}', Parquet)
        select number * 10 as u, intDiv(number, ${ROWS_PER_GROUP}) * 1000 + number % 4 as c
        from numbers(${ROW_GROUPS} * ${ROWS_PER_GROUP})
        settings output_format_parquet_bloom_filter_bits_per_value = ${bits_per_value};
    "
}

# Compare the bloom filter sizes of `c` and `u` in every row group. Nested `arrayJoin` and `tupleElement`
# calls instead of ARRAY JOIN clauses and dotted tuple access keep the query working with the old analyzer.
function check_folding()
{
    local file=$1
    ${CLICKHOUSE_CLIENT} --query="
        select
            countIf(u_bytes = 0 or c_bytes = 0) as row_groups_without_filter,
            countIf(c_bytes * 16 <= u_bytes) as row_groups_with_folded_c,
            count() as row_groups
        from (
            select
                sumIf(tupleElement(col, 'bloom_filter_bytes'), tupleElement(col, 'name') = 'c') as c_bytes,
                sumIf(tupleElement(col, 'bloom_filter_bytes'), tupleElement(col, 'name') = 'u') as u_bytes
            from (
                select tupleElement(rg, 'file_offset') as file_offset, arrayJoin(tupleElement(rg, 'columns')) as col
                from (select arrayJoin(row_groups) as rg from file('${file}', ParquetMetadata))
            )
            group by file_offset
        )
        format TSV;
    "
}

# One probe per every ${PROBE_STEP}-th row group, all of them in a single client invocation. Each probe
# prints the number of matching rows and the number of rows read; the read count is at least the whole
# row group holding the value and grows only by false positives in other row groups.
function probe()
{
    local file=$1
    local offset=$2
    local queries=""
    for ((g = 0; g < ROW_GROUPS; g += PROBE_STEP))
    do
        queries+="select count() as cnt from file('${file}', Parquet) where c = $((g * 1000 + offset)) format JSON;"
    done
    ${READ} --query="${queries}" | jq -r '[.data[0].cnt, .statistics.rows_read] | @tsv'
}

function summarize_present()
{
    awk -v n="${PROBES}" -v rows="${ROWS_PER_GROUP}" '
        $1 == rows / 4 { found++ }
        $2 >= rows { read_group++ }
        $2 == rows { read_exactly_one_group++ }
        END {
            printf "row groups whose matching value was found: %d of %d\n", found, n;
            printf "row groups read for present values: %d of %d\n", read_group, n;
            if (read_exactly_one_group >= n - 1)
                print "pruning still effective for present values";
            else
                printf "too many row groups read for present values: only %d probes read a single row group\n", read_exactly_one_group;
        }'
}

function summarize_absent()
{
    awk -v n="${PROBES}" '
        $1 == 0 && $2 == 0 { pruned++ }
        END {
            if (pruned >= n - 1)
                print "absent values pruned";
            else
                printf "absent values not pruned: %d of %d\n", pruned, n;
        }'
}

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
write_file "${DATA_FILE}" 10.5

echo "-- default bits per value: row groups without a filter, with a folded filter of c, total"
check_folding "${DATA_FILE}"
echo "-- default bits per value: probes for values of c that are present in exactly one row group"
probe "${DATA_FILE}" 1 | summarize_present
echo "-- default bits per value: probes for values of c that are absent everywhere"
probe "${DATA_FILE}" 7 | summarize_absent

# The values must also survive a much denser filter (fewer bits per value means folding is stopped
# earlier by the fill rate, but `c` still folds all the way down while `u` does not fold at all).
DATA_FILE_2="${CLICKHOUSE_TEST_UNIQUE_NAME}_dense.parquet"
write_file "${DATA_FILE_2}" 4

echo "-- 4 bits per value: row groups without a filter, with a folded filter of c, total"
check_folding "${DATA_FILE_2}"
echo "-- 4 bits per value: probes for values of c that are present in exactly one row group"
probe "${DATA_FILE_2}" 1 | summarize_present

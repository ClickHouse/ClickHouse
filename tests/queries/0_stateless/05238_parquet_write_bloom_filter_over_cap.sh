#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Parquet writer starts from a bloom filter sized for every value it is going to hash and folds it down
# afterwards. Readers accept at most 128 MiB (4 Mi blocks) of filter, so a column chunk with more hashed
# values than fit at the requested `bits_per_value` gets no bloom filter at all, however few distinct values
# it holds. This pins that contract: an oversized filter would be rejected by readers, and a filter clamped to
# the cap would silently have a worse false positive rate than requested.
#
# A large `bits_per_value` brings a small file past the cap (4 Mi * 256 / 1000 ~ 1.07M hashed values per
# column chunk), standing in for the ~100M values a real column chunk needs at the default setting.
ROW_GROUPS=2
ROWS_PER_GROUP=2000000
DISTINCT_PER_GROUP=8
BITS_PER_VALUE=1000

# One thread and one big block, so the row groups have exactly ${ROWS_PER_GROUP} rows each.
CH="${CLICKHOUSE_CLIENT} --output_format_parquet_row_group_size=${ROWS_PER_GROUP} --max_block_size=${ROWS_PER_GROUP} --min_insert_block_size_rows=${ROWS_PER_GROUP} --min_insert_block_size_bytes=0 --max_threads=1 --max_insert_threads=1 --output_format_parquet_write_bloom_filter=1 --engine_file_truncate_on_insert=1"

# Disable every other pruning method so only the bloom filter could decide which row groups are read.
READ="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_dictionary_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=1 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0"

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# `few` and `many` hash ${ROWS_PER_GROUP} values per column chunk, more than fit under the cap: `few` holds only
# ${DISTINCT_PER_GROUP} distinct values per row group, `many` holds all distinct values; neither gets a filter.
# `under_cap` is a control with the same distinct values as `few` but one array element per four rows, so it
# hashes a quarter as many values, stays under the cap and gets a (folded) filter.
${CH} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select
        toUInt16(intDiv(number, ${ROWS_PER_GROUP}) * ${DISTINCT_PER_GROUP} + number % ${DISTINCT_PER_GROUP}) as few,
        toUInt32(number) as many,
        if(number % 4 = 0, [few], []) as under_cap
    from numbers(${ROW_GROUPS} * ${ROWS_PER_GROUP})
    settings output_format_parquet_bloom_filter_bits_per_value = ${BITS_PER_VALUE};
"

echo "-- row groups without a filter for 'few', without a filter for 'many', with a filter for 'under_cap', total"
${CLICKHOUSE_CLIENT} --query="
    select
        countIf(few_bytes = 0) as row_groups_without_few_filter,
        countIf(many_bytes = 0) as row_groups_without_many_filter,
        countIf(under_cap_bytes > 0) as row_groups_with_under_cap_filter,
        count() as row_groups
    from (
        select
            sumIf(tupleElement(col, 'bloom_filter_bytes'), tupleElement(col, 'path') = 'few') as few_bytes,
            sumIf(tupleElement(col, 'bloom_filter_bytes'), tupleElement(col, 'path') = 'many') as many_bytes,
            sumIf(tupleElement(col, 'bloom_filter_bytes'), tupleElement(col, 'path') = 'under_cap.list.element') as under_cap_bytes
        from (
            select tupleElement(rg, 'file_offset') as file_offset, arrayJoin(tupleElement(rg, 'columns')) as col
            from (select arrayJoin(row_groups) as rg from file('${DATA_FILE}', ParquetMetadata))
        )
        group by file_offset
    )
    format TSV;
"

# The file must still read correctly, and without a filter nothing can be pruned: a probe for a value of `few`
# present in exactly one row group finds all its rows but reads the whole file, and so does a probe for a
# value absent everywhere. All probes run in one client invocation; each prints the matching rows and the
# rows read.
function probe()
{
    local queries=""
    for ((g = 0; g < ROW_GROUPS; ++g))
    do
        queries+="select count() as cnt from file('${DATA_FILE}', Parquet) where few = $((g * DISTINCT_PER_GROUP + 3)) format JSON;"
    done
    queries+="select count() as cnt from file('${DATA_FILE}', Parquet) where few = $((ROW_GROUPS * DISTINCT_PER_GROUP + 3)) format JSON;"
    ${READ} --query="${queries}" | jq -r '[.data[0].cnt, .statistics.rows_read] | @tsv'
}

echo "-- probes for a value of 'few' present in exactly one row group, then for an absent value: rows found, rows read"
probe | awk -v n="${ROW_GROUPS}" -v rows="${ROWS_PER_GROUP}" -v distinct="${DISTINCT_PER_GROUP}" '
    NR <= n && $1 == rows / distinct && $2 == n * rows { found++ }
    NR > n && $1 == 0 && $2 == n * rows { absent_full_scan = 1 }
    END {
        printf "present values found, reading the whole file: %d of %d\n", found, n;
        if (absent_full_scan)
            print "absent value not pruned, reading the whole file";
        else
            print "absent value pruned or miscounted";
    }'

#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Parquet writer starts from a bloom filter sized for every value it is going to hash and folds it down
# afterwards. Readers accept at most 128 MiB (4 Mi blocks) of filter, so a column chunk with more hashed
# values than fit at the requested `bits_per_value` cannot get a filter sized for all of them. It must not
# lose its filter for that reason alone: the chunk is usually far from unique at that scale, so the writer
# clamps the initial filter to the cap and folds from there. Only when the chunk really holds too many
# distinct values for the cap is the filter dropped, as the old distinct-count based sizing did.
#
# A large `bits_per_value` brings a small file past the cap (4 Mi * 256 / 1000 ~ 1.07M hashed values per
# column chunk), standing in for the ~100M values a real column chunk needs at the default setting.
ROW_GROUPS=2
ROWS_PER_GROUP=2000000
DISTINCT_PER_GROUP=8
BITS_PER_VALUE=1000

# One thread and one big block, so the row groups have exactly ${ROWS_PER_GROUP} rows each.
CH="${CLICKHOUSE_CLIENT} --output_format_parquet_row_group_size=${ROWS_PER_GROUP} --max_block_size=${ROWS_PER_GROUP} --min_insert_block_size_rows=${ROWS_PER_GROUP} --min_insert_block_size_bytes=0 --max_threads=1 --max_insert_threads=1 --output_format_parquet_write_bloom_filter=1 --engine_file_truncate_on_insert=1"

# Disable every other pruning method so only the bloom filter decides which row groups are read.
READ="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_dictionary_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=1 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0"

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# `few`: ${ROWS_PER_GROUP} hashed values per column chunk, but only ${DISTINCT_PER_GROUP} distinct ones, each
# specific to its row group. `many`: as many hashed values, all distinct, so no filter of the allowed size
# can hold them.
${CH} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select
        toUInt16(intDiv(number, ${ROWS_PER_GROUP}) * ${DISTINCT_PER_GROUP} + number % ${DISTINCT_PER_GROUP}) as few,
        toUInt32(number) as many
    from numbers(${ROW_GROUPS} * ${ROWS_PER_GROUP})
    settings output_format_parquet_bloom_filter_bits_per_value = ${BITS_PER_VALUE};
"

# ${DISTINCT_PER_GROUP} values fold down to a filter of a few hundred bytes; 4 KiB leaves room for the
# folding heuristic while still being nowhere near the 128 MiB cap.
echo "-- row groups with a small folded filter for 'few', without a filter for 'many', total"
${CLICKHOUSE_CLIENT} --query="
    select
        countIf(few_bytes > 0 and few_bytes <= 4096) as row_groups_with_small_few_filter,
        countIf(many_bytes = 0) as row_groups_without_many_filter,
        count() as row_groups
    from (
        select
            sumIf(tupleElement(col, 'bloom_filter_bytes'), tupleElement(col, 'name') = 'few') as few_bytes,
            sumIf(tupleElement(col, 'bloom_filter_bytes'), tupleElement(col, 'name') = 'many') as many_bytes
        from (
            select tupleElement(rg, 'file_offset') as file_offset, arrayJoin(tupleElement(rg, 'columns')) as col
            from (select arrayJoin(row_groups) as rg from file('${DATA_FILE}', ParquetMetadata))
        )
        group by file_offset
    )
    format TSV;
"

# The clamped and folded filter must still answer correctly: a probe for a value of `few` must find all
# its rows, and, with every other pruning method disabled, must read only the one row group holding it.
# A value that is absent everywhere must prune every row group. All probes run in one client invocation;
# each prints the matching rows and the rows read.
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
    NR <= n && $1 == rows / distinct && $2 == rows { exact++ }
    NR > n && $1 == 0 && $2 == 0 { pruned++ }
    END {
        printf "present values found and read from their single row group: %d of %d\n", exact, n;
        if (pruned == 1)
            print "absent value pruned everywhere";
        else
            print "absent value not pruned";
    }'

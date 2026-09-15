#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Parquet writer starts from a bloom filter sized for every value it is going to hash and folds it down
# afterwards. Readers accept at most 128 MiB (4 Mi blocks) of filter, so a column chunk with more hashed
# values than fit at the requested `bits_per_value` cannot get a filter sized for all of them. It must not
# lose its filter for that reason alone: the chunk is usually far from unique at that scale (the elements of
# an `Array` reach it easily), so the writer clamps the initial filter to the cap and folds from there.
# Only when the chunk really holds too many distinct values for the cap is the filter dropped, as the old
# distinct-count based sizing did.
#
# A large `bits_per_value` brings a small file past the cap (4 Mi * 256 / 1000 ~ 1.07M hashed values per
# column chunk), standing in for the ~100M values a real column chunk needs at the default setting.
ROW_GROUPS=2
ROWS_PER_GROUP=2000
ELEMENTS_PER_ROW=1000
DISTINCT_PER_GROUP=8
BITS_PER_VALUE=1000

# One thread and one big block, so the row groups have exactly ${ROWS_PER_GROUP} rows each.
CH="${CLICKHOUSE_CLIENT} --output_format_parquet_row_group_size=${ROWS_PER_GROUP} --max_block_size=1000000 --max_threads=1 --max_insert_threads=1 --output_format_parquet_write_bloom_filter=1 --engine_file_truncate_on_insert=1"

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# `few`: 2M hashed values per column chunk, but only ${DISTINCT_PER_GROUP} distinct ones, each specific to
# its row group. `many`: as many hashed values, all distinct, so no filter of the allowed size can hold them.
${CH} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select
        arrayMap(i -> toUInt16(intDiv(number, ${ROWS_PER_GROUP}) * ${DISTINCT_PER_GROUP} + i % ${DISTINCT_PER_GROUP}), range(${ELEMENTS_PER_ROW})) as few,
        arrayMap(i -> toUInt32(number * ${ELEMENTS_PER_ROW} + i), range(${ELEMENTS_PER_ROW})) as many
    from numbers(${ROW_GROUPS} * ${ROWS_PER_GROUP})
    settings output_format_parquet_bloom_filter_bits_per_value = ${BITS_PER_VALUE};
"

# Nested `arrayJoin` and `tupleElement` calls instead of ARRAY JOIN clauses and dotted tuple access keep
# the query working with the old analyzer. ${DISTINCT_PER_GROUP} values fold down to a filter of a few
# hundred bytes; 4 KiB leaves room for the folding heuristic while still being nowhere near the 128 MiB cap.
echo "-- row groups with a small folded filter for 'few', without a filter for 'many', total"
${CLICKHOUSE_CLIENT} --query="
    select
        countIf(few_bytes > 0 and few_bytes <= 4096) as row_groups_with_small_few_filter,
        countIf(many_bytes = 0) as row_groups_without_many_filter,
        count() as row_groups
    from (
        select
            sumIf(tupleElement(col, 'bloom_filter_bytes'), tupleElement(col, 'path') = 'few.list.element') as few_bytes,
            sumIf(tupleElement(col, 'bloom_filter_bytes'), tupleElement(col, 'path') = 'many.list.element') as many_bytes
        from (
            select tupleElement(rg, 'file_offset') as file_offset, arrayJoin(tupleElement(rg, 'columns')) as col
            from (select arrayJoin(row_groups) as rg from file('${DATA_FILE}', ParquetMetadata))
        )
        group by file_offset
    )
    format TSV;
"

# The folded filter must still answer correctly for every value it received.
echo "-- values of 'few' with exactly one row group worth of rows, distinct values"
${CLICKHOUSE_CLIENT} --query="
    select countIf(rows = ${ROWS_PER_GROUP} * ${ELEMENTS_PER_ROW} / ${DISTINCT_PER_GROUP}) as values_in_one_row_group, count() as values
    from (
        select value, count() as rows
        from (select arrayJoin(few) as value from file('${DATA_FILE}', Parquet))
        group by value
    )
    format TSV;
"

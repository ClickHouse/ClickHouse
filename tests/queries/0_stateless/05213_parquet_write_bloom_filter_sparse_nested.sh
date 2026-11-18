#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Parquet writer sizes the initial (not yet folded) bloom filter of a column chunk for the number of
# values it is going to hash, and writes no filter at all when that size would exceed the 128 MiB that
# readers accept. For a repeated or nullable leaf only the entries at the maximum definition level
# carry a value; the null and empty-array placeholders are never hashed. This checks that the placeholders
# do not count towards the size: a sparse `Array(Nullable(UInt64))` with one non-null element per row must
# get exactly the same filter as a plain `UInt64` column holding the same values, instead of none.
#
# 100 definition-level slots per row and a large `bits_per_value` bring the placeholder count past the cap
# with a small file, standing in for the ~100M placeholders a real column chunk needs at the default setting.
ROW_GROUPS=2
ROWS_PER_GROUP=12000
ELEMENTS_PER_ROW=100
BITS_PER_VALUE=1000

# One thread and one big block, so the row groups have exactly ${ROWS_PER_GROUP} rows each.
CH="${CLICKHOUSE_CLIENT} --output_format_parquet_row_group_size=${ROWS_PER_GROUP} --max_block_size=1000000 --max_threads=1 --max_insert_threads=1 --output_format_parquet_write_bloom_filter=1 --engine_file_truncate_on_insert=1"

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

${CH} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select
        number as dense,
        arrayMap(i -> if(i = 0, number, NULL), range(${ELEMENTS_PER_ROW})) as sparse
    from numbers(${ROW_GROUPS} * ${ROWS_PER_GROUP})
    settings output_format_parquet_bloom_filter_bits_per_value = ${BITS_PER_VALUE};
"

# Nested `arrayJoin` and `tupleElement` calls instead of ARRAY JOIN clauses and dotted tuple access keep
# the query working with the old analyzer.
echo "-- row groups without a filter, with equal filter sizes for dense and sparse, total"
${CLICKHOUSE_CLIENT} --query="
    select
        countIf(dense_bytes = 0 or sparse_bytes = 0) as row_groups_without_filter,
        countIf(dense_bytes = sparse_bytes) as row_groups_with_equal_filters,
        count() as row_groups
    from (
        select
            sumIf(tupleElement(col, 'bloom_filter_bytes'), tupleElement(col, 'path') = 'dense') as dense_bytes,
            sumIf(tupleElement(col, 'bloom_filter_bytes'), tupleElement(col, 'path') = 'sparse.list.element') as sparse_bytes
        from (
            select tupleElement(rg, 'file_offset') as file_offset, arrayJoin(tupleElement(rg, 'columns')) as col
            from (select arrayJoin(row_groups) as rg from file('${DATA_FILE}', ParquetMetadata))
        )
        group by file_offset
    )
    format TSV;
"

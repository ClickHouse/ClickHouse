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

CH="${CLICKHOUSE_CLIENT} --output_format_parquet_row_group_size=2000 --max_block_size=1000000 --output_format_parquet_write_bloom_filter=1 --engine_file_truncate_on_insert=1"

# Disable every other pruning method so only the bloom filter decides which row groups are read.
READ="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_dictionary_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=1 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0"

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 100 row groups of 2000 rows. `u` is distinct in every row, `d` has only 4 distinct values per chunk.
${CH} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select number * 10 as u, number % 4 as d from numbers(200000);
"

echo "the filter of the duplicate-heavy column is much smaller than the one of the distinct column"
${CLICKHOUSE_CLIENT} --query="
    select if(max(d_bytes) * 4 < min(u_bytes), 'yes', 'no: ' || toString(max(d_bytes)) || ' vs ' || toString(min(u_bytes)))
    from (
        select
            sumIf(col.bloom_filter_bytes, col.name = 'd') as d_bytes,
            sumIf(col.bloom_filter_bytes, col.name = 'u') as u_bytes
        from file('${DATA_FILE}', ParquetMetadata)
        array join row_groups as rg
        array join rg.columns as col
        group by rg.file_offset
    );
"

echo "every row group is written with a bloom filter"
${CLICKHOUSE_CLIENT} --query="
    select countIf(col.bloom_filter_bytes = 0), count()
    from file('${DATA_FILE}', ParquetMetadata) array join row_groups as rg array join rg.columns as col;
"

# One probe per row group: the value belongs to exactly one row group, so a sound filter keeps that
# row group (2000 rows read) and, with a reasonable false positive rate, prunes the other 99.
present_hits=0
present_rows_total=0
for ((i = 0; i < 50; ++i))
do
    value=$(( (i * 2 * 2000 + 1234) * 10 ))
    rows=$(${READ} --query="select count() from file('${DATA_FILE}', Parquet) where u = ${value} FORMAT JSON" | jq -r '.statistics.rows_read')
    if [ "${rows}" -ge 2000 ]
    then
        present_hits=$((present_hits + 1))
    fi
    present_rows_total=$((present_rows_total + rows))
done
echo "no false negatives: ${present_hits} of 50 row groups with a matching value were read"
# 50 probes that must read at least 2000 rows each; allow a generous margin for false positives.
if [ "${present_rows_total}" -le 400000 ]
then
    echo "pruning still effective for present values"
else
    echo "too many row groups read for present values: ${present_rows_total}"
fi

absent_pruned=0
for ((i = 0; i < 50; ++i))
do
    value=$(( i * 2 * 2000 * 10 + 3 ))
    rows=$(${READ} --query="select count() from file('${DATA_FILE}', Parquet) where u = ${value} FORMAT JSON" | jq -r '.statistics.rows_read')
    if [ "${rows}" -eq 0 ]
    then
        absent_pruned=$((absent_pruned + 1))
    fi
done
# Each of the 50 probes is tested against all 100 row groups, so a single false positive anywhere
# keeps a probe from being pruned completely; the filter is good enough that the vast majority of
# the probes are pruned everywhere.
if [ "${absent_pruned}" -ge 45 ]
then
    echo "absent values pruned"
else
    echo "absent values not pruned: ${absent_pruned} of 50"
fi

# The values must also survive a much denser filter (fewer bits per value means more folding).
DATA_FILE_2="${CLICKHOUSE_TEST_UNIQUE_NAME}_dense.parquet"
${CH} --query="
    insert into function file('${DATA_FILE_2}', Parquet)
    select number * 10 as u from numbers(200000)
    settings output_format_parquet_bloom_filter_bits_per_value = 4;
"
dense_hits=0
for ((i = 0; i < 50; ++i))
do
    value=$(( (i * 2 * 2000 + 777) * 10 ))
    rows=$(${READ} --query="select count() from file('${DATA_FILE_2}', Parquet) where u = ${value} FORMAT JSON" | jq -r '.statistics.rows_read')
    if [ "${rows}" -ge 2000 ]
    then
        dense_hits=$((dense_hits + 1))
    fi
done
echo "no false negatives with 4 bits per value: ${dense_hits} of 50"

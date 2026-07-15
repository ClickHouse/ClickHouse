#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for estimateAverageStringLengthPerRow() in the Parquet V3 reader.
# It used to multiply avg_dictionary_entry_size by num_values (including nulls),
# massively overestimating memory for dictionary-encoded Nullable(String) columns
# with high null rates and large dictionary entries.
#
# The test file (255 bytes) has a single optional BYTE_ARRAY column with:
#   - 354,100 rows, 354,099 nulls (1 non-null)
#   - Dictionary with 1 entry of ~120 KB
#   - No size_statistics (forces the dictionary estimation path)
#   - Has statistics.null_count
#
# Before fix: avg=120002 * 354100 / 354100 = 120002 bytes/row → 16 GiB reserve
# After fix:  avg=120002 * 1 / 354100 ≈ 0.3 bytes/row

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="$CUR_DIR/data_parquet/04099_dict_nullable_string_memory.parquet"
cp "$DATA_FILE" "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# Reading with the V3 reader should succeed within 5 GiB.
# Before the fix, this tried to allocate 16 GiB for ~120 KB of actual data.
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(length(ifNull(s, '')))
    FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet', Parquet, 's Nullable(String)')
    SETTINGS
        input_format_parquet_use_native_reader_v3 = 1,
        max_memory_usage = '5G'
"

rm -f "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

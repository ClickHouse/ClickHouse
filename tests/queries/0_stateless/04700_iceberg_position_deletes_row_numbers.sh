#!/usr/bin/env bash
# Tags: no-fasttest

# Position deletes are applied by mapping the deleted row numbers onto the physical rows of a
# chunk. When the reader already dropped rows itself (PREWHERE pushed down to the native Parquet
# reader), that mapping goes through `ChunkInfoRowNumbers.applied_filter`, so check the result of
# both readers, with and without a pushed-down filter.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "INSERT INTO ${TABLE} SELECT number, char(number % 26 + ascii('a')) FROM numbers(0, 1000)"

# Deletes both the very first and the very last row of the data file, plus a dense middle range,
# so that the deleted positions cover the borders of the row-number window of every chunk.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "ALTER TABLE ${TABLE} DELETE WHERE a % 7 = 0 OR a BETWEEN 300 AND 700 OR a = 999"

for roaring in 0 1; do
    for v3 in 0 1; do
        echo "roaring=${roaring} parquet_v3=${v3}:"
        ${CLICKHOUSE_CLIENT} \
            --use_roaring_bitmap_iceberg_positional_deletes=${roaring} \
            --input_format_parquet_use_native_reader_v3=${v3} \
            --max_block_size=100 \
            --query "SELECT count(), sum(a), min(a), max(a) FROM ${TABLE}"

        ${CLICKHOUSE_CLIENT} \
            --use_roaring_bitmap_iceberg_positional_deletes=${roaring} \
            --input_format_parquet_use_native_reader_v3=${v3} \
            --max_block_size=100 \
            --query "SELECT count(), sum(a), min(a), max(a) FROM ${TABLE} PREWHERE a % 3 = 1"
    done
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"

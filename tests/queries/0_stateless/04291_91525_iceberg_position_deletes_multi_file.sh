#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/91525
# Position deletes returned wrong results when:
#   * one delete file referenced multiple data files (typical of v2 merge-on-read),
#   * AND multiple delete files applied to the same data file.
# The streaming transform passed the WHERE filter to the Parquet reader, but that
# filter only drives row-group/page pruning, not row-level filtering. Rows for
# other data files survived inside surviving row groups and broke the streaming
# merge invariant that positions arrive in ascending order per delete source.

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

# Two inserts create two data files (rows 0..9 and 10..19).
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "INSERT INTO ${TABLE} SELECT number, char(number + ascii('a')) FROM numbers(0, 10)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "INSERT INTO ${TABLE} SELECT number, char(number + ascii('a')) FROM numbers(10, 10)"

# Two deletes; each delete touches both data files, producing one delete file
# per ALTER with rows that reference both data files.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "ALTER TABLE ${TABLE} DELETE WHERE a % 2 = 0"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "ALTER TABLE ${TABLE} DELETE WHERE a % 3 = 0"

# Expected survivors: 1, 5, 7, 11, 13, 17, 19 (7 rows). Verify in both
# streaming and roaring-bitmap modes, with and without the native Parquet v3 reader.
for roaring in 0 1; do
    for v3 in 0 1; do
        echo "roaring=${roaring} parquet_v3=${v3}:"
        ${CLICKHOUSE_CLIENT} \
            --use_roaring_bitmap_iceberg_positional_deletes=${roaring} \
            --input_format_parquet_use_native_reader_v3=${v3} \
            --query "SELECT count(), sum(a), groupArray(a) FROM (SELECT a FROM ${TABLE} ORDER BY a)"
    done
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"

#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_t0"

rm -rf "${ICEBERG_TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t0 (id Int32, name String, score Nullable(Int32), region String)
    ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH}', 'Parquet')
    PARTITION BY (region, icebergBucket(2, id))
    ORDER BY (score);
"

echo '--- system.iceberg_files schema ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT name, type
    FROM system.columns
    WHERE database = 'system' AND table = 'iceberg_files'
    ORDER BY position
    FORMAT TSV;
"

echo '--- per-file columns (empty table) ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        table,
        snapshot_id != 0                              AS snapshot_id_nonzero,
        content,
        endsWith(file_path, '.parquet')               AS file_path_ends_with_parquet,
        file_format,
        record_count,
        file_size_in_bytes > 0                        AS file_size_positive,
        partition,
        schema_id,
        sequence_number,
        sort_order_id,
        null_value_counts,
        value_counts,
        arraySort(mapKeys(column_sizes))              AS column_sizes_keys,
        arrayAll(x -> x > 0, mapValues(column_sizes)) AS column_sizes_all_positive,
        equality_ids
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 't0'
    ORDER BY partition
    FORMAT Vertical;
"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    INSERT INTO t0 VALUES (1, 'a', 10, 'us'), (2, 'b', NULL, 'us'), (3, 'c', 30, 'eu');
"

echo '--- per-file columns ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        table,
        snapshot_id != 0                              AS snapshot_id_nonzero,
        content,
        endsWith(file_path, '.parquet')               AS file_path_ends_with_parquet,
        file_format,
        record_count,
        file_size_in_bytes > 0                        AS file_size_positive,
        partition,
        schema_id,
        sequence_number,
        // TODO: sort_order_id is empty in the results because ClickHouse's Iceberg writer doesn't record it in the manifest.
        sort_order_id,
        null_value_counts,
        -- value_counts is empty: ClickHouse's Iceberg writer (IcebergWrites.cpp) does not emit f_value_counts today.
        -- See https://github.com/ClickHouse/ClickHouse/issues/103168.
        value_counts,
        arraySort(mapKeys(column_sizes))              AS column_sizes_keys,
        arrayAll(x -> x > 0, mapValues(column_sizes)) AS column_sizes_all_positive,
        equality_ids
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 't0'
    ORDER BY partition
    FORMAT Vertical;
"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    ALTER TABLE t0 DELETE WHERE id = 1;
"

echo '--- per-file columns for files added by the delete ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        table,
        content,
        endsWith(file_path, '-deletes.parquet') AS file_path_is_position_delete,
        file_format,
        record_count,
        file_size_in_bytes > 0                  AS file_size_positive,
        partition,
        sequence_number,
        equality_ids
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 't0' AND sequence_number > 1
    ORDER BY partition
    FORMAT Vertical;
"

rm -rf "${ICEBERG_TABLE_PATH}"

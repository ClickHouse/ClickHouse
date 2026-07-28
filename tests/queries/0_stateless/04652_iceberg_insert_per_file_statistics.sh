#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MULTI_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_multi"
SINGLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_single"
TYPES_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_types"
PART_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_part"

rm -rf "${MULTI_PATH}" "${SINGLE_PATH}" "${TYPES_PATH}" "${PART_PATH}"

# One row per data file, so the number of data files does not depend on randomized block sizes.
ONE_ROW_PER_FILE="
    SET allow_experimental_insert_into_iceberg = 1;
    SET iceberg_insert_max_rows_in_data_file = 1;
    SET max_insert_threads = 1, max_block_size = 1, max_insert_block_size = 1;
    SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
"

# Three data files with one row each; only the first row has a non-NULL 'score'.
${CLICKHOUSE_CLIENT} --query "
    ${ONE_ROW_PER_FILE}
    CREATE TABLE multi (id Int32, score Nullable(Int32))
    ENGINE = IcebergLocal('${MULTI_PATH}', 'Parquet') ORDER BY (id);
    INSERT INTO multi SELECT number + 1, if(number > 0, NULL, 10) FROM numbers(3);
"

# The same three rows in a single data file: the control for the common path.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    SET max_insert_threads = 1;
    CREATE TABLE single (id Int32, score Nullable(Int32))
    ENGINE = IcebergLocal('${SINGLE_PATH}', 'Parquet') ORDER BY (id);
    INSERT INTO single SELECT number + 1, if(number > 0, NULL, 10) FROM numbers(3);
"

echo '--- multi-file: no entry may report more nulls than the file has rows ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        count()                                                     AS files,
        countIf(null_value_counts[2] > record_count)                AS impossible_entries,
        arraySort(groupArray((record_count, null_value_counts[2])))  AS record_count_and_null_count
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'multi' AND content = 0
    FORMAT Vertical;
"

echo '--- multi-file: column_sizes describe one file, not the whole manifest ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        (SELECT max(column_sizes[1]) FROM system.iceberg_files
         WHERE database = currentDatabase() AND table = 'multi' AND content = 0)
        <
        (SELECT max(column_sizes[1]) FROM system.iceberg_files
         WHERE database = currentDatabase() AND table = 'single' AND content = 0)
        AS per_file_size_below_whole_manifest_size
    FORMAT TSV;
"

echo '--- single-file control: statistics unchanged ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        count()                                            AS files,
        any(record_count)                                  AS record_count,
        any(null_value_counts)                             AS null_counts,
        arraySort(mapKeys(any(column_sizes)))              AS column_sizes_keys,
        arrayAll(x -> x > 0, mapValues(any(column_sizes)))  AS column_sizes_all_positive
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'single' AND content = 0
    FORMAT Vertical;
"

# One data file per row across every statistics-bearing type the Iceberg writer supports,
# plus an all-NULL column whose bounds are omitted instead of throwing.
${CLICKHOUSE_CLIENT} --query "
    ${ONE_ROW_PER_FILE}
    CREATE TABLE types (id Int32, ni Nullable(Int32), ns Nullable(String), nl Nullable(Int64),
                        nf Nullable(Float64), nd Nullable(Date), all_null Nullable(Int32))
    ENGINE = IcebergLocal('${TYPES_PATH}', 'Parquet') ORDER BY (id);
    INSERT INTO types SELECT
        number + 1,
        if(number > 0, NULL, 10),
        if(number > 0, NULL, 'x'),
        if(number > 0, NULL, 100::Int64),
        if(number > 0, NULL, 1.5),
        if(number > 0, NULL, toDate('2020-01-01')),
        NULL
    FROM numbers(3);
"

echo '--- type matrix ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        count()                                                                                   AS files,
        countIf(arrayExists(k -> null_value_counts[k] > record_count, mapKeys(null_value_counts))) AS impossible_entries,
        arraySort(groupArray(null_value_counts))                                                  AS null_counts
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'types' AND content = 0
    FORMAT Vertical;
"

echo '--- type matrix: data reads back unchanged ---'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM types ORDER BY id FORMAT TSV;"

# Two partitions, two data files each: every partition writes its own manifest.
${CLICKHOUSE_CLIENT} --query "
    ${ONE_ROW_PER_FILE}
    CREATE TABLE part (region String, id Int32, score Nullable(Int32))
    ENGINE = IcebergLocal('${PART_PATH}', 'Parquet') PARTITION BY (region) ORDER BY (id);
    INSERT INTO part SELECT if(number < 2, 'eu', 'us'), number + 1, if(number % 2 = 0, 10, NULL) FROM numbers(4);
"

echo '--- partitioned ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        count()                                                                 AS files,
        countIf(null_value_counts[3] > record_count)                             AS impossible_entries,
        arraySort(groupArray((partition, record_count, null_value_counts[3])))   AS partition_record_count_null_count
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'part' AND content = 0
    FORMAT Vertical;
"

# ClickHouse's Iceberg writer does not emit 'value_counts' (issue #103168); routing per-file
# statistics through the verbatim-carryover channel instead would start emitting it.
echo '--- value_counts still not emitted ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT countIf(length(value_counts) > 0) AS entries_with_value_counts
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table IN ('multi', 'single', 'types', 'part') AND content = 0
    FORMAT TSV;
"

rm -rf "${MULTI_PATH}" "${SINGLE_PATH}" "${TYPES_PATH}" "${PART_PATH}"

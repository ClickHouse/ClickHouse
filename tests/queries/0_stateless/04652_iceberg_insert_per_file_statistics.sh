#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MULTI_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_multi"
SINGLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_single"
TYPES_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_types"
PART_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_part"
PAIRED_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_paired"
BOUNDED_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_bounded"
SIZED_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_sized"

rm -rf "${MULTI_PATH}" "${SINGLE_PATH}" "${TYPES_PATH}" "${PART_PATH}" "${PAIRED_PATH}" "${BOUNDED_PATH}" "${SIZED_PATH}"

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

# The statistics-to-file pairing is a bare index, so every assertion above (a sorted multiset or an
# aggregate) still holds if the per-file statistics are permuted relative to the data files, which is
# exactly the mis-pairing that reintroduces the unsafe pruning. This scenario joins each manifest
# entry to the Parquet file it names and compares against that file's real contents, so a permutation
# moves the output. `score` is non-NULL only in the file holding `id` = 1, and each row is ordered by
# the file's own `id` rather than by `file_path` because paths carry generated names.
${CLICKHOUSE_CLIENT} --query "
    ${ONE_ROW_PER_FILE}
    CREATE TABLE paired (id Int32, score Nullable(Int32))
    ENGINE = IcebergLocal('${PAIRED_PATH}', 'Parquet') ORDER BY (id);
    INSERT INTO paired SELECT number + 1, if(number > 0, NULL, 10) FROM numbers(3);
"

echo '--- per-entry pairing against the referenced Parquet file ---'
for manifest in $(find "${PAIRED_PATH}/metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f | sort); do
    # `lower_bounds`/`upper_bounds` hold raw little-endian bytes (`dumpValue` in `IcebergWrites.cpp`),
    # so they are decoded with `reinterpretAsInt32`; both key columns are `Int32` here. An entry whose
    # `score` is all-NULL legitimately has NO bounds at all: `canWriteStatistics` is all-or-nothing
    # across the entry's columns and `ColumnNullable::getExtremes` yields NULL extremes for an
    # all-NULL column, which `canDumpIcebergStats` rejects. That is pre-existing behaviour, so it is
    # asserted here rather than fixed.
    ${CLICKHOUSE_CLIENT} --query "
        WITH entries AS (
            SELECT
                replaceRegexpOne(tupleElement(data_file, 'file_path'), '^.*/', '')                     AS base,
                tupleElement(data_file, 'record_count')                                               AS entry_rows,
                CAST(tupleElement(data_file, 'null_value_counts'), 'Map(Int32, Int64)')[2]            AS entry_score_nulls,
                arrayMap(x -> (x.1, reinterpretAsInt32(x.2)), tupleElement(data_file, 'lower_bounds')) AS entry_lower,
                arrayMap(x -> (x.1, reinterpretAsInt32(x.2)), tupleElement(data_file, 'upper_bounds')) AS entry_upper
            FROM file('${manifest}', Avro)
        ),
        files AS (
            SELECT
                replaceRegexpOne(_path, '^.*/', '') AS base,
                any(id)                             AS own_id,
                count()                             AS file_rows,
                countIf(score IS NULL)              AS file_score_nulls
            FROM file('${PAIRED_PATH}/data/*.parquet', Parquet)
            GROUP BY base
        )
        SELECT
            'id=' || toString(f.own_id)
              || ' rows=' || toString(e.entry_rows) || '/' || toString(f.file_rows)
              || ' score_nulls=' || toString(e.entry_score_nulls) || '/' || toString(f.file_score_nulls)
              || ' paired=' || if((e.entry_rows = f.file_rows) AND (e.entry_score_nulls = f.file_score_nulls), 'yes', 'no')
              || ' lower=' || toString(e.entry_lower)
              || ' upper=' || toString(e.entry_upper) AS entry
        FROM entries AS e INNER JOIN files AS f ON e.base = f.base
        ORDER BY f.own_id
        FORMAT TSV;
    "
done

# The bounds above are present on one entry only, because the two all-NULL files legitimately carry no
# bounds at all. This companion has no nullable column, so every entry carries bounds and the decoded
# value of each is checked against the single row its own file holds. Without it the bounds half of
# this change would be pinned on a single entry.
${CLICKHOUSE_CLIENT} --query "
    ${ONE_ROW_PER_FILE}
    CREATE TABLE bounded (id Int32, v Int32)
    ENGINE = IcebergLocal('${BOUNDED_PATH}', 'Parquet') ORDER BY (id);
    INSERT INTO bounded SELECT number + 1, (number + 1) * 100 FROM numbers(3);
"

echo '--- per-entry bounds describe only their own file ---'
for manifest in $(find "${BOUNDED_PATH}/metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f | sort); do
    ${CLICKHOUSE_CLIENT} --query "
        WITH entries AS (
            SELECT
                replaceRegexpOne(tupleElement(data_file, 'file_path'), '^.*/', '')                     AS base,
                arrayMap(x -> (x.1, reinterpretAsInt32(x.2)), tupleElement(data_file, 'lower_bounds')) AS entry_lower,
                arrayMap(x -> (x.1, reinterpretAsInt32(x.2)), tupleElement(data_file, 'upper_bounds')) AS entry_upper
            FROM file('${manifest}', Avro)
        ),
        files AS (
            SELECT
                replaceRegexpOne(_path, '^.*/', '') AS base,
                any(id)                             AS own_id,
                any(v)                              AS own_v
            FROM file('${BOUNDED_PATH}/data/*.parquet', Parquet)
            GROUP BY base
        )
        SELECT
            'id=' || toString(f.own_id)
              || ' lower=' || toString(e.entry_lower)
              || ' upper=' || toString(e.entry_upper)
              || ' bounds_are_own_row=' || if(
                     e.entry_lower = [(1, f.own_id), (2, f.own_v)]
                     AND e.entry_upper = [(1, f.own_id), (2, f.own_v)], 'yes', 'no') AS entry
        FROM entries AS e INNER JOIN files AS f ON e.base = f.base
        ORDER BY f.own_id
        FORMAT TSV;
    "
done

# The two scenarios above pin `record_count`, `null_value_counts` and the bounds to the file each
# entry names, but not `column_sizes`, whose only other assertion is a cross-table `max()` inequality
# that constrains no individual entry. Both of their fixtures also give every file the same per-file
# sizes, so a permutation cannot move a size value there. This scenario gives each file a `String` of
# a different width, which makes the sizes distinct and therefore permutation-sensitive. The values
# are in-memory `IColumn::byteSize` sums, not Parquet file sizes, so they are deterministic:
# `ColumnString::byteSize` is `chars.size() + offsets.size() * sizeof(offsets[0])`, and `insertData`
# appends exactly `length` bytes with no terminator.
${CLICKHOUSE_CLIENT} --query "
    ${ONE_ROW_PER_FILE}
    CREATE TABLE sized (id Int32, s String)
    ENGINE = IcebergLocal('${SIZED_PATH}', 'Parquet') ORDER BY (id);
    INSERT INTO sized SELECT number + 1, repeat('x', (number + 1) * 4) FROM numbers(3);
"

echo '--- per-entry column_sizes describe only their own file ---'
for manifest in $(find "${SIZED_PATH}/metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f | sort); do
    ${CLICKHOUSE_CLIENT} --query "
        WITH entries AS (
            SELECT
                replaceRegexpOne(tupleElement(data_file, 'file_path'), '^.*/', '')          AS base,
                CAST(tupleElement(data_file, 'column_sizes'), 'Map(Int32, Int64)') AS entry_sizes
            FROM file('${manifest}', Avro)
        ),
        files AS (
            SELECT
                replaceRegexpOne(_path, '^.*/', '') AS base,
                any(id)                             AS own_id,
                any(length(s))                      AS own_width
            FROM file('${SIZED_PATH}/data/*.parquet', Parquet)
            GROUP BY base
        )
        SELECT
            'id=' || toString(f.own_id)
              || ' width=' || toString(f.own_width)
              || ' id_size=' || toString(e.entry_sizes[1])
              || ' s_size=' || toString(e.entry_sizes[2]) AS entry
        FROM entries AS e INNER JOIN files AS f ON e.base = f.base
        ORDER BY f.own_id
        FORMAT TSV;
    "
done

rm -rf "${MULTI_PATH}" "${SINGLE_PATH}" "${TYPES_PATH}" "${PART_PATH}" "${PAIRED_PATH}" "${BOUNDED_PATH}" "${SIZED_PATH}"

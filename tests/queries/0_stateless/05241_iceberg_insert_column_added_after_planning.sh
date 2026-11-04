#!/usr/bin/env bash
# Tags: no-fasttest

# An INSERT that was planned before a column was added to the table must still commit, and the
# manifest entry it writes must describe only the columns its data file actually carries. Reading
# the table through a remembered older metadata file gives the narrower structure deterministically,
# which is the same divergence a concurrent ADD COLUMN produces.
#
# Parquet because it is the only format that reports per-column on-disk sizes, and clickhouse-local
# because the write used to terminate the process: in the shared test server that trips the
# hung-check instead of failing this test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE_PATH="${TABLE_DIR}/t/"
rm -rf "${TABLE_DIR}"
mkdir -p "${TABLE_PATH}"

LOCAL="${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0"

# One column, three rows: commits a metadata version whose schema has a single field.
${LOCAL} --query "
    CREATE TABLE t (number Int32) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    INSERT INTO t SELECT toInt32(number) FROM numbers(3);
"

# Remember that version, then widen the table.
PIN="metadata/$(basename "$(ls "${TABLE_PATH}metadata"/*.metadata.json | sort -V | tail -1)")"
${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    ALTER TABLE t ADD COLUMN note Nullable(String);
"

# The structure comes from the remembered version, one column narrow; the write goes to the widened
# table. A table read through a remembered version cannot see the snapshot its own write creates,
# so everything is asserted below through a fresh, unremembered attachment.
${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
        SETTINGS iceberg_metadata_file_path = '${PIN}';
    INSERT INTO t SELECT toInt32(number + 10) FROM numbers(3);
"

${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    SELECT 'rows', arraySort(groupArray(number)) FROM t;
    SELECT 'added_column_nulls', countIf(note IS NULL) FROM t;
    SELECT
        'files', count(),
        'size_keys_per_file', arraySort(groupArray(arraySort(mapKeys(column_sizes)))),
        'sizes_positive', min(arrayAll(x -> x > 0, mapValues(column_sizes))),
        'null_counts_per_file', arraySort(groupArray(null_value_counts))
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 't' AND content = 0;
"

rm -rf "${TABLE_DIR}"

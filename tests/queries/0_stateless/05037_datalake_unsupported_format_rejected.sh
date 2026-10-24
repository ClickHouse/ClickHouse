#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

# A data lake engine can only store its data files in the formats its own table
# format specification defines, and its reader never consults the file extension
# to decide how to parse a data file. An explicit `format` outside that set is
# therefore rejected at CREATE time, the same way `compression_method` is:
#  - on write it commits a data file no reader of that table format can parse,
#    corrupting the table permanently;
#  - on read it silently returns garbage rows instead of throwing.
#
# `DeltaLake` accepts `Parquet` only (its log records no per-file format);
# `Iceberg` and `Paimon` also accept `ORC` and `Avro`; `Hudi` base files are
# columnar, so `Parquet` and `ORC`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PREFIX="t_${CLICKHOUSE_DATABASE}_${RANDOM}"

TABLES=(
    "${TABLE_PREFIX}_iceberg_rowbinary"
    "${TABLE_PREFIX}_iceberg_csv"
    "${TABLE_PREFIX}_iceberg_orc"
    "${TABLE_PREFIX}_iceberg_default"
    "${TABLE_PREFIX}_iceberg_parquet_lower"
    "${TABLE_PREFIX}_attach_rowbinary"
    "${TABLE_PREFIX}_paimon_rowbinary"
    "${TABLE_PREFIX}_paimon_avro"
    "${TABLE_PREFIX}_hudi_avro"
    "${TABLE_PREFIX}_hudi_orc"
)

for table in "${TABLES[@]}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
done

# 1. A row format is rejected for `Iceberg`.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_iceberg_rowbinary (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_iceberg_rowbinary', 'RowBinary')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 2. So is a text format.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_iceberg_csv (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_iceberg_csv', 'CSV')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 3. `ORC` is part of the Iceberg specification, so it is accepted. The engine
#    error message must be absent (`grep -c` returns 0).
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_iceberg_orc (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_iceberg_orc', 'ORC')
" 2>&1 | grep -c "is not supported by the" || true
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${TABLE_PREFIX}_iceberg_orc"

# 4. No `format` argument at all keeps working - the check never fires for the
#    `auto` default, which is resolved to the lake's own default afterwards.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_iceberg_default (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_iceberg_default')
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_PREFIX}_iceberg_default"

# 5. The comparison is case-insensitive, as everywhere else for format names.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_iceberg_parquet_lower (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_iceberg_parquet_lower', 'parquet')
" 2>&1 | grep -c "is not supported by the" || true
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${TABLE_PREFIX}_iceberg_parquet_lower"

# 6. Table-function path: rejected too, because table functions always load with
#    `LoadingStrictnessLevel::CREATE`. The rejection fires while parsing the
#    arguments, before any file is touched, so a non-existent path is fine.
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM icebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_tf_rowbinary', 'RowBinary', 'c0 Int32')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 7. `DeltaLake` accepts `Parquet` only, so `ORC` - fine for `Iceberg` above - is
#    rejected here. This is the reported corruption: the Delta log carries no
#    per-file format, so every reader parses the data files as `Parquet`.
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM deltaLakeLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_tf_delta_orc', 'ORC', 'c0 Int32')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 8. `ATTACH` stays exempt, like for `compression_method`: a table persisted
#    before this validation landed must still load after an upgrade. The
#    format-specific error must be absent (`grep -c` returns 0); the statement
#    may still fail because the path holds no Iceberg metadata, which does not
#    print that message.
mkdir -p "${USER_FILES_PATH:?}/${TABLE_PREFIX}_attach_rowbinary"
${CLICKHOUSE_CLIENT} --query "
    ATTACH TABLE ${TABLE_PREFIX}_attach_rowbinary
    FROM '${USER_FILES_PATH}/${TABLE_PREFIX}_attach_rowbinary'
    (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_attach_rowbinary', 'RowBinary')
" 2>&1 | grep -c "is not supported by the" || true

# 9. `Paimon` rejects a row format, like `Iceberg`.
${CLICKHOUSE_CLIENT} --allow_experimental_paimon_storage_engine 1 --query "
    CREATE TABLE ${TABLE_PREFIX}_paimon_rowbinary (c0 Int)
    ENGINE = PaimonLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_paimon_rowbinary', 'RowBinary')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 10. `Avro` is part of the Paimon specification, so it is accepted. This is the
#     case that separates `Paimon` from `Hudi` and `DeltaLake` below: were the
#     `Paimon` branch dropped, it would fall back to `Parquet` only and fail here.
#     Only the absence of the format error is asserted, as in case 8: `Paimon`
#     reads its schema while creating the table, so the statement still fails on
#     an empty directory for every format, `Parquet` included. Case 9 is the
#     control proving this message does appear for `Paimon` when the format is
#     outside the set.
${CLICKHOUSE_CLIENT} --allow_experimental_paimon_storage_engine 1 --query "
    CREATE TABLE ${TABLE_PREFIX}_paimon_avro (c0 Int)
    ENGINE = PaimonLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_paimon_avro', 'Avro')
" 2>&1 | grep -c "is not supported by the" || true

# 11. `Hudi` base files are columnar, so `Avro` - accepted for `Iceberg` and
#     `Paimon` above - is rejected here. The rejection fires while parsing the
#     arguments, before S3 is contacted; the endpoint is a closed local port so
#     that it refuses immediately rather than hanging if that ever changes.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_hudi_avro (c0 Int)
    ENGINE = Hudi('http://127.0.0.1:1/bucket/${TABLE_PREFIX}_hudi_avro', 'k', 's', 'Avro')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 12. `ORC` is accepted for `Hudi`, unlike for `DeltaLake` in case 7.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_hudi_orc (c0 Int)
    ENGINE = Hudi('http://127.0.0.1:1/bucket/${TABLE_PREFIX}_hudi_orc', 'k', 's', 'ORC')
" 2>&1 | grep -c "is not supported by the" || true
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${TABLE_PREFIX}_hudi_orc"

# Cleanup.
for table in "${TABLES[@]}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    rm -rf "${USER_FILES_PATH:?}/${table}"
done

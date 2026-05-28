#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/105644
# DataLake engines (Iceberg, DeltaLake, Hudi, Paimon) must reject the
# `compression_method` argument at CREATE TIME because the data file format
# (Parquet/ORC/Avro) already carries its own internal codec. The outer codec
# is silently dropped on the Iceberg write path while still applied on read,
# yielding files the engine cannot read back; for the other data lake formats
# it produces non-standard files that external readers cannot decode.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PREFIX="t_${CLICKHOUSE_DATABASE}_${RANDOM}"

TABLES=(
    "${TABLE_PREFIX}_lzma"
    "${TABLE_PREFIX}_gzip"
    "${TABLE_PREFIX}_default"
    "${TABLE_PREFIX}_none"
    "${TABLE_PREFIX}_auto"
    "${TABLE_PREFIX}_auto_upper"
    "${TABLE_PREFIX}_none_mixed"
    "${TABLE_PREFIX}_attach_lzma"
    "${TABLE_PREFIX}_attach_gzip"
)

for table in "${TABLES[@]}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
done

# 1. Positional `compression_method = 'lzma'` is rejected.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_lzma (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_lzma', 'Parquet', 'lzma')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 2. Positional `compression_method = 'gzip'` is also rejected (general policy).
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_gzip (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_gzip', 'Parquet', 'gzip')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 3. Default (no compression argument) is accepted.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_default (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_default')
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_PREFIX}_default"

# 4. Explicit `compression_method = 'none'` is accepted.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_none (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_none', 'Parquet', 'none')
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_PREFIX}_none"

# 5. Explicit `compression_method = 'auto'` is accepted.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_auto (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_auto', 'Parquet', 'auto')
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_PREFIX}_auto"

# 6. `AUTO` (upper case) is accepted: validation is case-insensitive and the stored
#    value is canonicalized to lowercase so downstream chooseCompressionMethod calls
#    do not later throw `Unknown compression method`.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_auto_upper (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_auto_upper', 'Parquet', 'AUTO')
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_PREFIX}_auto_upper"

# 7. `None` (mixed case) is accepted with the same canonicalization.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_none_mixed (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_none_mixed', 'Parquet', 'None')
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_PREFIX}_none_mixed"

# 8 / 9. `ATTACH` path: existing tables persisted before this validation landed can
#        carry a non-default `compression_method` in their metadata. The rejection
#        above must be gated on `LoadingStrictnessLevel < ATTACH` so server restart
#        and explicit `ATTACH` can still load such tables. We use the deprecated
#        Ordinary engine because the Atomic engine forbids the full-spec
#        `ATTACH TABLE name (cols) ENGINE = ...` form used to simulate a metadata
#        replay. For each table we count occurrences of the rejection error; the
#        expected count is 0 (rejection did not fire).
ORD_DB="${TABLE_PREFIX}_ord"
${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --query "
    CREATE DATABASE ${ORD_DB} ENGINE = Ordinary
"

${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --query "
    ATTACH TABLE ${ORD_DB}.t_attach_lzma (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_attach_lzma', 'Parquet', 'lzma')
" 2>&1 | grep -c "not supported by data lake engines" || true

${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 --query "
    ATTACH TABLE ${ORD_DB}.t_attach_gzip (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_attach_gzip', 'Parquet', 'gzip')
" 2>&1 | grep -c "not supported by data lake engines" || true

# Cleanup.
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${ORD_DB} SYNC"
for table in "${TABLES[@]}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    rm -rf "${USER_FILES_PATH:?}/${table}"
done

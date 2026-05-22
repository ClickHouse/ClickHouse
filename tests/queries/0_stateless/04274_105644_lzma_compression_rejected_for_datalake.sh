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

for table in "${TABLE_PREFIX}_lzma" "${TABLE_PREFIX}_gzip" "${TABLE_PREFIX}_default" "${TABLE_PREFIX}_none" "${TABLE_PREFIX}_auto"; do
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

# Cleanup.
for table in "${TABLE_PREFIX}_lzma" "${TABLE_PREFIX}_gzip" "${TABLE_PREFIX}_default" "${TABLE_PREFIX}_none" "${TABLE_PREFIX}_auto"; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    rm -rf "${USER_FILES_PATH:?}/${table}"
done

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
#        and explicit `ATTACH` can still load such tables. We simulate this by
#        creating with allowed `'auto'`, detaching, rewriting the on-disk metadata
#        to inject the forbidden value, and reattaching. ATTACH must succeed without
#        the rejection error (grep returns 0) AND the table must actually exist
#        afterwards (EXISTS returns 1). The positive existence check guards against
#        ATTACH failing for any unrelated reason where grep alone would still print 0.
DEFAULT_DISK_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name = 'default'")
for forbidden in lzma gzip; do
    ATTACH_TABLE="${TABLE_PREFIX}_attach_${forbidden}"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${ATTACH_TABLE} (c0 Int)
        ENGINE = IcebergLocal('${USER_FILES_PATH}/${ATTACH_TABLE}', 'Parquet', 'auto')
    "
    METADATA_REL=$(${CLICKHOUSE_CLIENT} --query "
        SELECT metadata_path FROM system.tables WHERE database = currentDatabase() AND name = '${ATTACH_TABLE}'
    ")
    METADATA_ABS="${DEFAULT_DISK_PATH}${METADATA_REL}"
    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${ATTACH_TABLE}"
    sed -i "s|'auto'|'${forbidden}'|" "${METADATA_ABS}"
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${ATTACH_TABLE}" 2>&1 \
        | grep -c "not supported by data lake engines" || true
    ${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${ATTACH_TABLE}"
done

# 10. Table-function path: data lake table functions also call `initialize` with
#     the default `CREATE` mode through `TableFunctionObjectStorage::parseArgumentsImpl`,
#     so the rejection fires for them too. Arg order for the table function is
#     `path, format, structure, compression_method`, so we pass an explicit structure
#     before the forbidden codec. The rejection fires during argument parsing before
#     any file access, so a non-existent path is fine.
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM icebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_tf_lzma', 'Parquet', 'c0 Int32', 'lzma')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 11. Same for `'gzip'` via the table-function path.
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM icebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_tf_gzip', 'Parquet', 'c0 Int32', 'gzip')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# Cleanup.
for table in "${TABLES[@]}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    rm -rf "${USER_FILES_PATH:?}/${table}"
done

#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/105644
# Data lake engines (`Iceberg`, `DeltaLake`, `Hudi`, `Paimon`) must reject the
# `compression_method` argument at CREATE TIME because the data file format
# (`Parquet`/`ORC`/`Avro`) already carries its own internal codec. Any
# user-supplied wrapper is silently dropped on the Iceberg write path while
# still applied on read, yielding files the engine cannot read back; for the
# other data lake formats it produces non-standard files that external readers
# cannot decode.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PREFIX="t_${CLICKHOUSE_DATABASE}_${RANDOM}"

# Named collections are server-global, so the names are scoped to this run.
COLLECTION_LZMA="${CLICKHOUSE_TEST_UNIQUE_NAME}_nc_lzma"
COLLECTION_CLEAN="${CLICKHOUSE_TEST_UNIQUE_NAME}_nc_clean"
COLLECTION_S3="${CLICKHOUSE_TEST_UNIQUE_NAME}_nc_s3"
COLLECTION_S3_CLEAN="${CLICKHOUSE_TEST_UNIQUE_NAME}_nc_s3_clean"
COLLECTION_AZURE="${CLICKHOUSE_TEST_UNIQUE_NAME}_nc_azure"
COLLECTION_AZURE_CLEAN="${CLICKHOUSE_TEST_UNIQUE_NAME}_nc_azure_clean"

TABLES=(
    "${TABLE_PREFIX}_lzma"
    "${TABLE_PREFIX}_gzip"
    "${TABLE_PREFIX}_default"
    "${TABLE_PREFIX}_none"
    "${TABLE_PREFIX}_auto"
    "${TABLE_PREFIX}_auto_upper"
    "${TABLE_PREFIX}_none_mixed"
    "${TABLE_PREFIX}_attach_full_def_lzma"
    "${TABLE_PREFIX}_kv_compression"
    "${TABLE_PREFIX}_kv_compression_method"
    "${TABLE_PREFIX}_kv_compression_named"
    "${TABLE_PREFIX}_nc_lzma"
    "${TABLE_PREFIX}_nc_clean"
    "${TABLE_PREFIX}_nc_s3"
    "${TABLE_PREFIX}_nc_s3_clean"
    "${TABLE_PREFIX}_nc_azure"
    "${TABLE_PREFIX}_nc_azure_clean"
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

# 3. Default (no compression argument) is accepted: the rejection only fires
#    when the user explicitly supplied the argument, so the default path is
#    unaffected.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_default (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_default')
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_PREFIX}_default"

# 4. Explicit `compression_method = 'none'` is also rejected: any user-supplied
#    value is meaningless because the file format codec is the authority.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_none (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_none', 'Parquet', 'none')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 5. Explicit `compression_method = 'auto'` is also rejected (same reason).
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_auto (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_auto', 'Parquet', 'auto')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 6. Case-insensitive: `AUTO` (upper case) is rejected because the
#    rejection only checks whether the argument was supplied at all, not its
#    value.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_auto_upper (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_auto_upper', 'Parquet', 'AUTO')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 7. `None` (mixed case) is rejected for the same reason.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_none_mixed (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_none_mixed', 'Parquet', 'None')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 9. Short `ATTACH TABLE name` of metadata that already carries the forbidden
#    argument needs the server's on-disk metadata to be rewritten, which a
#    stateless test must not do; it lives in the integration test
#    `test_storage_iceberg_no_spark/test_datalake_compression_attach.py`.
#
# 9b. Full-definition `ATTACH TABLE name FROM 'path' (cols) ENGINE = ...` also
#     skips the rejection: the gate fires only for `LoadingStrictnessLevel ==
#     CREATE`, and every `ATTACH` form (short or full-definition) is
#     deliberately exempt as a compatibility path, so pre-fix tables can be
#     re-attached after upgrade even though a full-definition `ATTACH`
#     supplies the engine args inline. The
#     compression-specific error must be absent (`grep -c` returns 0); the
#     statement may still fail for an unrelated reason (the path holds no
#     Iceberg metadata), which does not print that message.
mkdir -p "${USER_FILES_PATH:?}/${TABLE_PREFIX}_attach_full_def_lzma"
${CLICKHOUSE_CLIENT} --query "
    ATTACH TABLE ${TABLE_PREFIX}_attach_full_def_lzma
    FROM '${USER_FILES_PATH}/${TABLE_PREFIX}_attach_full_def_lzma'
    (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_attach_full_def_lzma', 'Parquet', 'lzma')
" 2>&1 | grep -c "not supported by data lake engines" || true

# 10. Table-function path: data lake table functions also call `initialize`
#     with the default `CREATE` mode through
#     `TableFunctionObjectStorage::parseArgumentsImpl`, so the rejection
#     fires for them too. Arg order for the table function is
#     `path, format, structure, compression_method`, so we pass an explicit
#     structure before the forbidden codec. The rejection fires during
#     argument parsing before any file access, so a non-existent path is
#     fine.
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM icebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_tf_lzma', 'Parquet', 'c0 Int32', 'lzma')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 11. Same for `'gzip'` via the table-function path.
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM icebergLocal('${USER_FILES_PATH}/${TABLE_PREFIX}_tf_gzip', 'Parquet', 'c0 Int32', 'gzip')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 12 / 13 / 14. Key-value form via the `S3` data lake parser:
#         `IcebergS3('<url>', compression = 'lzma')` is the bot-reported gap
#         (PR #105667 inline review on `S3/Configuration.cpp`). Without the
#         alias coverage, the historical `compression` alias slipped past the
#         data lake rejection because the key-value reader only looked up the
#         canonical `compression_method` key, leaving
#         `compression_method_user_provided = false` and the rejection
#         bypassed.
#
#         The rejection fires during argument parsing in
#         `StorageObjectStorageConfiguration::initialize`, before any S3
#         client is constructed, so the URL only needs to be syntactically
#         well-formed (no network access happens).
#
# 12: alias form `compression = ...`.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_kv_compression (c0 Int)
    ENGINE = IcebergS3('http://localhost:11111/test/${TABLE_PREFIX}_kv_compression', compression = 'lzma')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 13: canonical form `compression_method = ...` (regression coverage so the
#     alias addition cannot silently shadow the canonical key).
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_kv_compression_method (c0 Int)
    ENGINE = IcebergS3('http://localhost:11111/test/${TABLE_PREFIX}_kv_compression_method', compression_method = 'lzma')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 14: alias inside a positional + key-value mix (positional `format` first,
#     then key-value `compression = ...`). The S3 parser must still find the
#     alias even when the canonical lookup also has a positional fallback
#     available.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_kv_compression_named (c0 Int)
    ENGINE = IcebergS3('http://localhost:11111/test/${TABLE_PREFIX}_kv_compression_named', 'Parquet', compression = 'lzma')
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 15. Named-collection carrier. Cases 12 to 14 pass the argument inline, so they
#     exercise the key-value branch of the parser but never
#     `fromNamedCollection`, which resolves the collection and marks the
#     provenance separately. A collection carrying `compression` must reach the
#     same rejection.
${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${COLLECTION_LZMA} AS
        path = '${USER_FILES_PATH}/${TABLE_PREFIX}_nc_lzma', format = 'Parquet', compression = 'lzma'
"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_nc_lzma (c0 Int) ENGINE = IcebergLocal(${COLLECTION_LZMA})
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# 16. Control for case 15: the same collection without `compression` is accepted
#     and reads back, so case 15 attributes to the argument rather than to the
#     named-collection carrier being rejected as such.
${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${COLLECTION_CLEAN} AS
        path = '${USER_FILES_PATH}/${TABLE_PREFIX}_nc_clean', format = 'Parquet'
"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_nc_clean (c0 Int) ENGINE = IcebergLocal(${COLLECTION_CLEAN})
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_PREFIX}_nc_clean"

# 17. Second carrier: the `S3` collection parser is independent of the `Local` one
#     in case 15, and this uses the canonical `compression_method` key rather than
#     the `compression` alias, so neither the parser nor the spelling of case 15
#     covers it. The message is matched instead of the `BAD_ARGUMENTS` name because
#     an unreachable bucket reports `BAD_ARGUMENTS` as well, so the name alone
#     cannot tell the rejection apart from a failure to list the bucket.
${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${COLLECTION_S3} AS
        url = 'http://localhost:11111/test/${TABLE_PREFIX}_nc_s3', compression_method = 'lzma'
"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_nc_s3 (c0 Int) ENGINE = IcebergS3(${COLLECTION_S3})
" 2>&1 | grep -o -m1 "not supported by data lake engines"

# 18. Control for case 17: the same collection without the key must not produce the
#     compression message (`grep -c` returns 0). The statement still fails, because
#     the URL resolves to nothing, and that failure carries a different message.
${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${COLLECTION_S3_CLEAN} AS
        url = 'http://localhost:11111/test/${TABLE_PREFIX}_nc_s3_clean'
"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_nc_s3_clean (c0 Int) ENGINE = IcebergS3(${COLLECTION_S3_CLEAN})
" 2>&1 | grep -c "not supported by data lake engines" || true

# 19. Third carrier: the `Azure` collection parser, back on the `compression` alias.
#     The endpoint only has to be well-formed, because the rejection precedes the
#     Azure client.
${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${COLLECTION_AZURE} AS
        blob_path = '${TABLE_PREFIX}_nc_azure', container = 'cont',
        storage_account_url = 'http://localhost:11112/devstoreaccount1', compression = 'lzma'
"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_nc_azure (c0 Int) ENGINE = IcebergAzure(${COLLECTION_AZURE})
" 2>&1 | grep -o -m1 "not supported by data lake engines"

# 20. Control for case 19: without the key the statement fails while authenticating
#     to the endpoint instead, so the compression message is absent.
${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${COLLECTION_AZURE_CLEAN} AS
        blob_path = '${TABLE_PREFIX}_nc_azure_clean', container = 'cont',
        storage_account_url = 'http://localhost:11112/devstoreaccount1'
"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PREFIX}_nc_azure_clean (c0 Int) ENGINE = IcebergAzure(${COLLECTION_AZURE_CLEAN})
" 2>&1 | grep -c "not supported by data lake engines" || true

# The `HDFS` collection parser carries the same assignment and is left uncovered
# here: no stateless test drives `IcebergHDFS`, and the `use-hdfs` tag that would
# admit one applies to the whole file, so every case above would stop running in
# Cloud.

# Cleanup.
for table in "${TABLES[@]}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    rm -rf "${USER_FILES_PATH:?}/${table}"
done
for collection in "${COLLECTION_LZMA}" "${COLLECTION_CLEAN}" "${COLLECTION_S3}" "${COLLECTION_S3_CLEAN}" \
                  "${COLLECTION_AZURE}" "${COLLECTION_AZURE_CLEAN}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${collection}"
done

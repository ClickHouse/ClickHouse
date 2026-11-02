#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"

# Regression test for #121514: an Iceberg sort-order field naming a transform that
# ClickHouse cannot parse used to dereference a disengaged std::optional in
# getSortingKeyDescriptionFromMetadata and abort the whole server process.
# The sort order must be dropped instead, so that the table stays readable.
# The table is created with use_iceberg_metadata_files_cache = 0: IcebergMetadata
# captures the metadata files cache into its persistent table components when
# the table is constructed, and IcebergStorageSink reuses that pointer on every
# INSERT regardless of the query settings. Without this, the INSERT below could
# be served the old cached metadata.json and never see the rewritten transform.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    CREATE TABLE ${TABLE} (x Int64, y String)
        ENGINE = IcebergLocal('${TABLE_PATH}')
        ORDER BY icebergTruncate(3, x)
        SETTINGS iceberg_format_version = 2;
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --query "INSERT INTO ${TABLE} VALUES (10, 'a'), (20, 'b')"

# Rewrite the transform in the latest metadata file to a value that
# parseTransformAndArgument cannot handle ('bucket' with a non-numeric
# argument returns std::nullopt). use_iceberg_metadata_files_cache is disabled
# on the reads so the rewritten metadata.json is really re-parsed instead of
# being served from the cache (the cache key does not change on an in-place
# rewrite of the same file).
LATEST=$(ls "${TABLE_PATH}metadata"/v*.metadata.json | sort -V | tail -1)
sed -i 's/"truncate\[3\]"/"bucket[xyz]"/' "${LATEST}"

# Reading the table must still work without the sorted-read optimization,
# and must not kill the server.
${CLICKHOUSE_CLIENT} --query "SELECT x, y FROM icebergLocal('${TABLE_PATH}', 'Parquet') ORDER BY x SETTINGS use_iceberg_metadata_files_cache = 0"

# The server must still be alive.
${CLICKHOUSE_CLIENT} --query "SELECT 1"

# A malformed spelling that takes the throwing path in parseTransformAndArgument
# ('bucket]' has no '[' and raises BAD_ARGUMENTS) must also be dropped instead
# of rejecting the table.
sed -i 's/"bucket\[xyz\]"/"bucket]"/' "${LATEST}"
${CLICKHOUSE_CLIENT} --query "SELECT x, y FROM icebergLocal('${TABLE_PATH}', 'Parquet') ORDER BY x SETTINGS use_iceberg_metadata_files_cache = 0"
${CLICKHOUSE_CLIENT} --query "SELECT 1"

# The write path must tolerate the dropped sort order too: IcebergStorageSink
# parses the sort order of the latest metadata through
# getSortingKeyDescriptionFromMetadata on every INSERT, so inserting a row
# into the table over the corrupted metadata used to fail the same way.
# The table is not recreated: CREATE over a path that already has Iceberg
# metadata throws TABLE_ALREADY_EXISTS.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --query "INSERT INTO ${TABLE} VALUES (30, 'c')"
${CLICKHOUSE_CLIENT} --query "SELECT x, y FROM icebergLocal('${TABLE_PATH}', 'Parquet') ORDER BY x SETTINGS use_iceberg_metadata_files_cache = 0"
${CLICKHOUSE_CLIENT} --query "SELECT 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"

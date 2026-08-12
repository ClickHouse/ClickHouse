#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `DatabaseOverlay` did not override `checkTableNameLength`, so in `clickhouse-local` a long
# `--default_database` left the limit unchecked while the overlay forwarded the create to a real
# on-disk database. The table was accepted and then could not be dropped, because the
# `metadata_dropped` filename derived from the database name exceeds NAME_MAX.

WORKING_FOLDER="${CLICKHOUSE_TMP}/04884_clickhouse_local_overlay_table_name_length"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

# 214 characters saturates the per-table budget to 0, so every table name is rejected.
LONG_DB=$(printf 'd%.0s' $(seq 1 214))

echo "--- long default_database: CREATE TABLE is rejected up front ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/long_table" \
    -q "CREATE TABLE tc (a UInt8) ENGINE = MergeTree ORDER BY a" \
    -- --default_database="${LONG_DB}" 2>&1 | grep -o -m1 'ARGUMENT_OUT_OF_BOUND'

echo "--- long default_database: CREATE VIEW is rejected the same way ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/long_view" \
    -q "CREATE VIEW v AS SELECT 1" \
    -- --default_database="${LONG_DB}" 2>&1 | grep -o -m1 'ARGUMENT_OUT_OF_BOUND'

echo "--- long default_database: RENAME TABLE to a rejected name is refused ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/long_rename" \
    -q "CREATE TABLE t0 (a UInt8) ENGINE = MergeTree ORDER BY a; RENAME TABLE t0 TO t1" \
    -- --default_database="${LONG_DB}" 2>&1 | grep -o -m1 'ARGUMENT_OUT_OF_BOUND'

# The limit is read back through the overlay, so this arm follows the disk's real NAME_MAX.
SHORT_DB="db04884"
ALLOWED_LENGTH=$(${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/probe" \
    -q "SELECT getMaxTableNameLengthForDatabase(currentDatabase())" \
    -- --default_database="${SHORT_DB}" | tr -d '[:space:]')

USE_S3_PLAIN_REWRITEABLE_AS_DB_DISK=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.disks WHERE name='disk_db_remote' AND type = 'ObjectStorage' AND object_storage_type='S3' AND metadata_type='PlainRewritable'" | tr -d '[:space:]')
# When using s3_plain_rewriteable as a db_disk, minio doesn't allow the path segment to have more than 255 characters
# Refer: https://github.com/minio/minio/blob/ddd9a84cd769e6bed67f5fe860f8f3c7527a6971/cmd/xl-storage.go#L154-L167
if [ "${USE_S3_PLAIN_REWRITEABLE_AS_DB_DISK}" == "0" ]; then
    echo "--- short default_database: a name at exactly the limit still works ---"
    ALLOWED_NAME=$(printf 't%.0s' $(seq 1 "${ALLOWED_LENGTH}"))
    ${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/boundary" \
        -q "CREATE TABLE ${ALLOWED_NAME} (a UInt8) ENGINE = MergeTree ORDER BY a; DROP TABLE ${ALLOWED_NAME}; SELECT 'boundary ok'" \
        -- --default_database="${SHORT_DB}"

    # escapeForFileName emits 3 bytes per non-word byte, so the check must measure the escaped name.
    echo "--- short default_database: the escaped length is what counts ---"
    ESCAPED_FIT=$(printf -- '-%.0s' $(seq 1 $((ALLOWED_LENGTH / 3))))
    ${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/escaped" \
        -q "CREATE TABLE \`${ESCAPED_FIT}\` (a UInt8) ENGINE = MergeTree ORDER BY a; DROP TABLE \`${ESCAPED_FIT}\`; SELECT 'escaped ok'" \
        -- --default_database="${SHORT_DB}"
    ESCAPED_OVER=$(printf -- '-%.0s' $(seq 1 $((ALLOWED_LENGTH / 3 + 1))))
    ${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/escaped_over" \
        -q "CREATE TABLE \`${ESCAPED_OVER}\` (a UInt8) ENGINE = MergeTree ORDER BY a" \
        -- --default_database="${SHORT_DB}" 2>&1 | grep -o -m1 'ARGUMENT_OUT_OF_BOUND'
fi

echo "--- short default_database: an existing table still loads in a later run ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/persist" \
    -q "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x; INSERT INTO t VALUES (42)" \
    -- --default_database="${SHORT_DB}"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/persist" \
    -q "SELECT x FROM t" \
    -- --default_database="${SHORT_DB}"

rm -rf "${WORKING_FOLDER}"

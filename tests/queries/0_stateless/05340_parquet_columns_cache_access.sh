#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it to reset its admission control

# The entries of a `Parquet` file in `system.columns_cache` are visible only to a user who may read
# from the source of the file, besides holding `SHOW COLUMNS` globally.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"
USER="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet) SELECT number AS id FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"

QUERY="SELECT count() > 0 FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}')"

function populate()
{
    # The admission control of the cache can skip a write: read the file until the entries are there.
    for _ in {1..20}; do
        $CLICKHOUSE_CLIENT -q "SELECT sum(id) FROM file('${FILE}') FORMAT Null
            SETTINGS use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1,
                columns_cache_max_bytes_to_write_to_cache = 0, columns_cache_max_estimated_bytes_to_write_to_cache = 0"
        [ "$($CLICKHOUSE_CLIENT -q "${QUERY}")" = 1 ] && return
    done
    echo "the entries of the file are not in the cache"
}

# Other tests may have lowered the admission probability of the cache; dropping the cache resets it.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${USER};
    CREATE USER ${USER} IDENTIFIED WITH no_password;
    GRANT SELECT ON system.columns_cache TO ${USER};
    GRANT SHOW COLUMNS ON *.* TO ${USER};"

echo "--- the default user"
populate
$CLICKHOUSE_CLIENT -q "${QUERY}"
echo "--- SHOW COLUMNS without READ ON FILE"
populate
$CLICKHOUSE_CLIENT --user "${USER}" -q "${QUERY}"
echo "--- READ ON S3 does not show the entries of a local file"
$CLICKHOUSE_CLIENT -q "GRANT READ ON S3 TO ${USER}"
populate
$CLICKHOUSE_CLIENT --user "${USER}" -q "${QUERY}"
echo "--- READ ON FILE"
$CLICKHOUSE_CLIENT -q "GRANT READ ON FILE TO ${USER}"
populate
$CLICKHOUSE_CLIENT --user "${USER}" -q "${QUERY}"
echo "--- READ ON FILE without SHOW COLUMNS"
$CLICKHOUSE_CLIENT -q "REVOKE SHOW COLUMNS ON *.* FROM ${USER}"
populate
$CLICKHOUSE_CLIENT --user "${USER}" -q "${QUERY}"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
rm -f "${FILE_PATH}"

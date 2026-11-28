#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it to reset its admission control

# The entries of a `Parquet` file read by a table (here `ENGINE = File`) belong to the table in
# `system.columns_cache`: they are visible only to a user who may see the table, not to a user who
# may only read from the source of the file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"
USER="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
# A user that never had `READ ON FILE`: with the replicated access storage (as in the `DatabaseReplicated`
# test configuration) `GRANT READ ON FILE` reads back as `GRANT FILE ON *.*`, which `REVOKE READ ON FILE`
# does not remove.
USER2="user2_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet) SELECT number AS id FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_parquet_file;
    CREATE TABLE t_parquet_file (id UInt64) ENGINE = File(Parquet, '${FILE}');"
TABLE_UUID=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_parquet_file'")

QUERY="SELECT count() > 0 FROM system.columns_cache WHERE endsWith(part, '/${FILE}')"
QUERY_TABLE="SELECT DISTINCT database = currentDatabase(), table FROM system.columns_cache WHERE endsWith(part, '/${FILE}') AND table != ''"

function populate_table_function()
{
    for _ in {1..20}; do
        $CLICKHOUSE_CLIENT -q "SELECT sum(id) FROM file('${FILE}', Parquet, 'id UInt64') FORMAT Null
            SETTINGS use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1,
                columns_cache_max_bytes_to_write_to_cache = 0, columns_cache_max_estimated_bytes_to_write_to_cache = 0"
        [ "$($CLICKHOUSE_CLIENT -q "${QUERY} AND table_uuid = toUUID('00000000-0000-0000-0000-000000000000')")" = 1 ] && return
    done
    echo "the entries of the table function are not in the cache"
}

function populate()
{
    # The admission control of the cache can skip a write: read the file until the entries are there.
    for _ in {1..20}; do
        $CLICKHOUSE_CLIENT -q "SELECT sum(id) FROM t_parquet_file FORMAT Null
            SETTINGS use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1,
                columns_cache_max_bytes_to_write_to_cache = 0, columns_cache_max_estimated_bytes_to_write_to_cache = 0"
        [ "$($CLICKHOUSE_CLIENT -q "${QUERY} AND table != ''")" = 1 ] && return
    done
    echo "the entries of the table are not in the cache"
}

# Other tests may have lowered the admission probability of the cache; dropping the cache resets it.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${USER};
    CREATE USER ${USER} IDENTIFIED WITH no_password;
    GRANT SELECT ON system.columns_cache TO ${USER};
    GRANT READ ON FILE TO ${USER};"

echo "--- a read by the table function warmed first does not give its entries to the table"
populate_table_function
populate
$CLICKHOUSE_CLIENT --user "${USER}" -q "SELECT countIf(table_uuid != toUUID('00000000-0000-0000-0000-000000000000')) FROM system.columns_cache WHERE endsWith(part, '/${FILE}')"
echo "--- the default user sees the entries of the table"
populate
$CLICKHOUSE_CLIENT -q "${QUERY_TABLE}"
echo "--- READ ON FILE without access to the table"
populate
$CLICKHOUSE_CLIENT --user "${USER}" -q "${QUERY} AND table != ''"
echo "--- SHOW COLUMNS on the table"
$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.t_parquet_file TO ${USER}"
populate
$CLICKHOUSE_CLIENT --user "${USER}" -q "${QUERY} AND table != ''"

QUERY_DROPPED="SELECT count() > 0 FROM system.columns_cache WHERE endsWith(part, '/${FILE}') AND table_uuid = toUUID('${TABLE_UUID}')"
echo "--- the entries of a dropped table: global SHOW COLUMNS without READ ON FILE"
$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${USER2};
    CREATE USER ${USER2} IDENTIFIED WITH no_password;
    GRANT SELECT ON system.columns_cache TO ${USER2};
    GRANT SHOW COLUMNS ON *.* TO ${USER2};"
populate
$CLICKHOUSE_CLIENT -q "DROP TABLE t_parquet_file SYNC"
# With `distributed_ddl_output_mode = none` (as in the `DatabaseReplicated` test configuration) the
# query returns before the table is dropped: wait until its UUID no longer resolves.
for _ in {1..60}; do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns_cache WHERE table_uuid = toUUID('${TABLE_UUID}') AND table != ''")" = 0 ] && break
    sleep 0.5
done
$CLICKHOUSE_CLIENT --user "${USER2}" -q "${QUERY_DROPPED}"
echo "--- the entries of a dropped table: global SHOW COLUMNS and READ ON FILE"
$CLICKHOUSE_CLIENT -q "GRANT READ ON FILE TO ${USER2}"
$CLICKHOUSE_CLIENT --user "${USER2}" -q "${QUERY_DROPPED}"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER}, ${USER2}"
rm -f "${FILE_PATH}"

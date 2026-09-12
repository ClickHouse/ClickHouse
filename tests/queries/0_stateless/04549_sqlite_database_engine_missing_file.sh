#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/04549_sqlite_db_missing_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"
CREATED_DB_PATH="${BASE}/created.sqlite"
ATTACHED_DB="db_04549_attached_${CLICKHOUSE_DATABASE}"
CREATED_DB="db_04549_created_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${ATTACHED_DB}"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CREATED_DB}"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

# The attach probes the missing file without creating it and logs the failure; the log line is
# forwarded to the client by `send_logs_level`, so keep it out of the test output.
${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE ${ATTACHED_DB} ENGINE = SQLite('${DB_PATH}')" 2>/dev/null

# Table enumeration must not throw while the database file is missing: one broken SQLite database
# must not fail `SHOW TABLES` or queries over `system.tables` that enumerate all databases
# (the same tolerance as the PostgreSQL and MySQL database engines). It must not fabricate an
# empty database file either. Direct table lookup, in contrast, fails closed.
echo 'Table discovery of a broken database is empty:'
${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${ATTACHED_DB}"
echo 'system.tables enumeration does not throw:'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${ATTACHED_DB}'"
echo 'Table lookup fails closed:'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${ATTACHED_DB}.t1" 2>&1 | grep -oF 'Cannot access sqlite database' | head -1

if [[ -e "${DB_PATH}" ]]; then
    echo 'SQLite database file was created by ATTACH'
else
    echo 'SQLite database file was not created by ATTACH'
fi

# Once the file appears, the already-attached database starts working without a re-attach.
sqlite3 "${DB_PATH}" 'CREATE TABLE t1(x INTEGER); INSERT INTO t1 VALUES (42);'
echo 'Tables after the file appeared:'
${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${ATTACHED_DB}"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${ATTACHED_DB}.t1"

# A genuine CREATE DATABASE may still create a missing database file.
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CREATED_DB} ENGINE = SQLite('${CREATED_DB_PATH}')"
if [[ -e "${CREATED_DB_PATH}" ]]; then
    echo 'SQLite database file was created by CREATE DATABASE'
else
    echo 'SQLite database file was not created by CREATE DATABASE'
fi

#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: requires the SQLite library, which is not built in the fast test.
# no-parallel: dealing with an SQLite database makes concurrent SHOW TABLES queries fail sporadically with the "database is locked" error.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CURR_DATABASE="test_04891_sqlite_${CLICKHOUSE_DATABASE}"
DB_PATH="${USER_FILES_PATH}/${CURR_DATABASE}.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query="DETACH DATABASE IF EXISTS ${CURR_DATABASE}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS ${CURR_DATABASE}" 2>/dev/null
    rm -f "${DB_PATH}"
}
trap cleanup EXIT

rm -f "${DB_PATH}"

# An `ATTACH DATABASE` over a missing file must not create it: table access fails closed. But the
# ClickHouse-side proxy must still be removable, because `DETACH DATABASE` and `DROP DATABASE` only
# drop ClickHouse metadata and never touch the SQLite file. Both go through
# `DatabaseCatalog::detachDatabase(check_empty = true)`, which asks the database whether it is empty.
# The `ATTACH` probes the missing file without creating it and logs the failure; the log line is
# forwarded to the client by `send_logs_level`, so keep it out of the test output.
${CLICKHOUSE_CLIENT} --query="ATTACH DATABASE ${CURR_DATABASE} ENGINE = SQLite('${DB_PATH}')" 2>/dev/null

echo 'The missing file was not created by ATTACH:'
test -e "${DB_PATH}" && echo 'exists' || echo 'does not exist'

echo 'Direct table access fails closed:'
${CLICKHOUSE_CLIENT} --query="SELECT * FROM ${CURR_DATABASE}.some_table" 2>&1 | grep -oF 'Cannot access sqlite database' | head -1

echo 'DETACH DATABASE succeeds while the file is still missing:'
${CLICKHOUSE_CLIENT} --query="DETACH DATABASE ${CURR_DATABASE}"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.databases WHERE name = '${CURR_DATABASE}'"

echo 'DROP DATABASE succeeds while the file is still missing:'
${CLICKHOUSE_CLIENT} --query="ATTACH DATABASE ${CURR_DATABASE}" 2>/dev/null
${CLICKHOUSE_CLIENT} --query="DROP DATABASE ${CURR_DATABASE}"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.databases WHERE name = '${CURR_DATABASE}'"

echo 'The SQLite file was never created:'
test -e "${DB_PATH}" && echo 'exists' || echo 'does not exist'

echo 'A database over an existing file still reports its tables:'
sqlite3 "${DB_PATH}" "CREATE TABLE t (x INTEGER);"
sqlite3 "${DB_PATH}" "INSERT INTO t VALUES (1);"
chmod ugo+w "${DB_PATH}"
${CLICKHOUSE_CLIENT} --query="ATTACH DATABASE ${CURR_DATABASE} ENGINE = SQLite('${DB_PATH}')"
${CLICKHOUSE_CLIENT} --query="SHOW TABLES FROM ${CURR_DATABASE}"
${CLICKHOUSE_CLIENT} --query="SELECT x FROM ${CURR_DATABASE}.t"

echo 'DROP DATABASE removes the proxy and keeps the SQLite file:'
${CLICKHOUSE_CLIENT} --query="DROP DATABASE ${CURR_DATABASE}"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.databases WHERE name = '${CURR_DATABASE}'"
test -e "${DB_PATH}" && echo 'the SQLite file is kept' || echo 'the SQLite file is gone'

#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/04538_sqlite_query_missing_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04538"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

sqlite3 "${DB_PATH}" 'CREATE TABLE source(id INTEGER); INSERT INTO source VALUES (1);'

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04538 ENGINE = SQLite('${DB_PATH}', query('SELECT id FROM source'))"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_04538"
rm -f "${DB_PATH}"
# The re-attach probes the missing file without creating it and logs the failure; the log line is
# forwarded to the client by `send_logs_level`, so keep it out of the test output.
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_04538" 2>/dev/null

# A query-backed `SQLite` storage is read-only. Its first read after `ATTACH` must fail closed while the
# database is unavailable instead of creating an empty database as a side effect.
${CLICKHOUSE_CLIENT} --query "SELECT id FROM t_04538" 2>&1 | grep -oF 'Cannot access sqlite database' | head -1

if [[ -e "${DB_PATH}" ]]; then
    echo 'SQLite database file was created'
else
    echo 'SQLite database file was not created'
fi

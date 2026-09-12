#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/05137_sqlite_create_missing_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05137_table"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05137_query"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05137_explicit"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

# A `CREATE TABLE` without a column list has to infer the schema from the remote table or query, so a missing
# database file can never make the statement succeed. It must fail without leaving an empty database behind.
for source in "'source'" "query('SELECT id FROM source')"; do
    if [[ "${source}" == "'source'" ]]; then
        table=t_05137_table
    else
        table=t_05137_query
    fi

    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} ENGINE = SQLite('${DB_PATH}', ${source})" 2>&1 \
        | grep -oF -m1 'Cannot access sqlite database'

    if [[ -e "${DB_PATH}" ]]; then
        echo 'SQLite database file was created'
    else
        echo 'SQLite database file was not created'
    fi

    ${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${table}"
done

# An explicit column list does not need the remote schema, so a `CREATE TABLE` still materializes the file.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05137_explicit (id Int32) ENGINE = SQLite('${DB_PATH}', 'source')"

if [[ -e "${DB_PATH}" ]]; then
    echo 'SQLite database file was created'
else
    echo 'SQLite database file was not created'
fi

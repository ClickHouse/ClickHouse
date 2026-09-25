#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/04903_sqlite_attach_missing_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04903_table"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04903_query"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

for source in "'source'" "query('SELECT id FROM source')"; do
    if [[ "${source}" == "'source'" ]]; then
        table=t_04903_table
    else
        table=t_04903_query
    fi

    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${table} ENGINE = SQLite('${DB_PATH}', ${source})" 2>&1 \
        | grep -oF -m1 'Cannot access sqlite database'
done

if [[ -e "${DB_PATH}" ]]; then
    echo 'SQLite database file was created'
else
    echo 'SQLite database file was not created'
fi

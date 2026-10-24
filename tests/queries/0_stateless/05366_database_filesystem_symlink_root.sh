#!/usr/bin/env bash

# On a plain `user_files_path`, the root of a `Filesystem` database is checked after resolving
# symlinks: an in-root symlink to an outside directory must not become the database root,
# while a symlink to a directory inside `user_files` is accepted.
# https://github.com/ClickHouse/ClickHouse/pull/100173

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXTERNAL_DIR=$(realpath "${CLICKHOUSE_TMP}")/${CLICKHOUSE_TEST_UNIQUE_NAME}_outside
INSIDE_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_inside"
ESCAPE_LINK="${CLICKHOUSE_TEST_UNIQUE_NAME}_escape"
INSIDE_LINK="${CLICKHOUSE_TEST_UNIQUE_NAME}_inside_link"
DB="db_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}"
    rm -f "${USER_FILES_PATH}/${ESCAPE_LINK}" "${USER_FILES_PATH}/${INSIDE_LINK}"
    rm -rf "${EXTERNAL_DIR}" "${INSIDE_DIR}"
}
trap cleanup EXIT

mkdir -p "${EXTERNAL_DIR}" "${INSIDE_DIR}"
echo 1 > "${EXTERNAL_DIR}/data.csv"
echo 2 > "${INSIDE_DIR}/data.csv"
chmod -R 777 "${EXTERNAL_DIR}" "${INSIDE_DIR}"
ln -s "${EXTERNAL_DIR}" "${USER_FILES_PATH}/${ESCAPE_LINK}"
ln -s "${INSIDE_DIR}" "${USER_FILES_PATH}/${INSIDE_LINK}"

echo '--- a root that is a symlink to an outside directory'
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB} ENGINE = Filesystem('${ESCAPE_LINK}')" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

echo '--- a root that is a symlink to a directory inside user_files'
${CLICKHOUSE_CLIENT} -q "
CREATE DATABASE ${DB} ENGINE = Filesystem('${INSIDE_LINK}');
SELECT * FROM ${DB}.\`data.csv\`;
"

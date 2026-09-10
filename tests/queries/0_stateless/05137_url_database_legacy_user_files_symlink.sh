#!/usr/bin/env bash

# The `user_files` boundary check of a `URL('file://')` database decides whether the existence of a
# table is probed at all. It must be the same check the delegated `file` storage applies: on a plain
# `user_files_path` (no `user_files_policy`) an admin-managed symlink inside the directory is
# accepted by the delegate, so resolving the path here would make the catalog claim tables it cannot
# resolve - `EXISTS TABLE` would answer 1 for a file that does not exist.
# https://github.com/ClickHouse/ClickHouse/pull/100173

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXTERNAL_DIR=$(realpath "${CLICKHOUSE_TMP}")/${CLICKHOUSE_TEST_UNIQUE_NAME}_url_target
LINK_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_url_link"
LINK_PATH="${USER_FILES_PATH}/${LINK_NAME}"
DB="db_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}"
    rm -f "${LINK_PATH}"
    rm -rf "${EXTERNAL_DIR}"
}
trap cleanup EXIT

mkdir -p "${EXTERNAL_DIR}"
printf '1,one\n2,two\n' > "${EXTERNAL_DIR}/data.csv"
chmod -R 777 "${EXTERNAL_DIR}"
ln -s "${EXTERNAL_DIR}" "${LINK_PATH}"

${CLICKHOUSE_CLIENT} -q "
DROP DATABASE IF EXISTS ${DB};
CREATE DATABASE ${DB} ENGINE = URL('file://');
"

echo '--- EXISTS TABLE through a symlink inside user_files'
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.\`${LINK_NAME}/data.csv\`"

echo '--- EXISTS TABLE of a missing file through the same symlink'
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.\`${LINK_NAME}/missing.csv\`"

echo '--- DESCRIBE through a symlink inside user_files'
# The inferred types depend on the settings the test run randomizes, so pin the one that decides
# whether the columns of an inferred schema are nullable.
${CLICKHOUSE_CLIENT} --schema_inference_make_columns_nullable 1 -q "DESCRIBE TABLE ${DB}.\`${LINK_NAME}/data.csv\`" | cut -f 1,2

echo '--- SELECT through a symlink inside user_files'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB}.\`${LINK_NAME}/data.csv\` ORDER BY ALL"

echo '--- a missing file through the same symlink reports that it does not exist'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB}.\`${LINK_NAME}/missing.csv\`" 2>&1 | grep -o -m1 -E 'UNKNOWN_TABLE|FILE_DOESNT_EXIST'

#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Fast tests don't build external libraries (SQLite)

# On a plain `user_files_path` (no `user_files_policy`), an admin-managed symlink inside the
# directory is an established way to expose an external location, and every consumer of
# `user_files` accepts it. `openSQLiteDB` must keep that contract: the stricter resolved-path
# containment check applies only when `user_files_policy` is configured.
# https://github.com/ClickHouse/ClickHouse/pull/100173

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXTERNAL_DIR=$(realpath "${CLICKHOUSE_TMP}")/${CLICKHOUSE_TEST_UNIQUE_NAME}_sqlite_target
LINK_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_sqlite_link"
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
sqlite3 "${EXTERNAL_DIR}/db.sqlite" 'CREATE TABLE tbl (x INTEGER); INSERT INTO tbl VALUES (1), (2);'
chmod -R 777 "${EXTERNAL_DIR}"
ln -s "${EXTERNAL_DIR}" "${LINK_PATH}"

echo '--- the `sqlite` table function through a symlink inside user_files'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM sqlite('${LINK_NAME}/db.sqlite', 'tbl') ORDER BY ALL"

echo '--- `ENGINE = SQLite` through a symlink inside user_files'
${CLICKHOUSE_CLIENT} -q "
DROP DATABASE IF EXISTS ${DB};
CREATE DATABASE ${DB} ENGINE = SQLite('${LINK_NAME}/db.sqlite');
SHOW TABLES FROM ${DB};
SELECT * FROM ${DB}.tbl ORDER BY ALL;
"

echo '--- a path escaping user_files is still rejected'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM sqlite('../../../../etc/passwd', 'something')" 2>&1 | grep -o -m1 'PATH_ACCESS_DENIED'

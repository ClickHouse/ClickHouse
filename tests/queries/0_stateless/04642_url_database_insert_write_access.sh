#!/usr/bin/env bash

# An INSERT into a table of a URL database writes to the underlying source, so besides the INSERT
# privilege on the logical table name it must require the write source access on the underlying
# source (e.g. WRITE ON FILE), the same way the corresponding table function does.
# https://github.com/ClickHouse/ClickHouse/pull/111512

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
DATA_FILE="${CLICKHOUSE_USER_FILES_UNIQUE}/data.csv"
printf '1\n' > "${DATA_FILE}"
chmod 666 "${DATA_FILE}"

DB="db_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE="${DATA_FILE}"

# Note: the source grants are given one by one, and the write is only exercised as "no source grant"
# vs. "the source is granted". A `READ`-only source grant cannot be relied upon here: with
# `access_control_improvements.enable_read_write_grants` disabled (the default), `GRANT READ ON FILE`
# is stored as the old-style whole-source `FILE` grant, which allows writing as well.
${CLICKHOUSE_CLIENT} -q "
DROP DATABASE IF EXISTS ${DB};
DROP USER IF EXISTS ${USER};
CREATE DATABASE ${DB} ENGINE = URL('file://');
CREATE USER ${USER} IDENTIFIED WITH no_password;
GRANT SELECT, INSERT ON ${DB}.* TO ${USER};
"

echo '--- SELECT without the read source grant must fail'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`${TABLE}\`" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo '--- INSERT without the write source grant must fail'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "INSERT INTO ${DB}.\`${TABLE}\` VALUES (2)" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo '--- INSERT without the write source grant must fail with an asynchronous insert too'
# The sink of an asynchronous insert is created in a background flush, so a check done only when
# the sink is created would neither reach the user (the query has already returned success with
# `wait_for_async_insert = 0`) nor run with the privileges the user had when the query was issued.
${CLICKHOUSE_CLIENT} --user "${USER}" --async_insert 1 --wait_for_async_insert 0 -q "INSERT INTO ${DB}.\`${TABLE}\` VALUES (2)" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo '--- SELECT with the read source grant'
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO ${USER}"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`${TABLE}\`"

echo '--- INSERT with the write source grant'
${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON FILE TO ${USER}"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "INSERT INTO ${DB}.\`${TABLE}\` VALUES (2)"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`${TABLE}\` ORDER BY ALL"

echo '--- INSERT into a plain file name keeps working in clickhouse-local'
LOCAL_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local.csv"
printf '1\n' > "${LOCAL_FILE}"
${CLICKHOUSE_LOCAL} -q "INSERT INTO \`${LOCAL_FILE}\` VALUES (2)"
cat "${LOCAL_FILE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER ${USER};
DROP DATABASE ${DB};
"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}" "${LOCAL_FILE}"

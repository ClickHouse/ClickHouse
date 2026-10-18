#!/usr/bin/env bash

# A table of a `Filesystem` database is a `file` table function under a logical name, so it must
# obey the same source access contract as that table function:
#  * the existence of the backing file must not be probed before the read source grant is checked,
#    otherwise `EXISTS TABLE` (which needs only `SHOW TABLES`) and the shape of the error of a
#    `SELECT` turn the database into an oracle for the contents of `user_files`;
#  * a resolved table is cached under its logical name, so a cache entry warmed by a privileged
#    query must not let an unprivileged user read the file;
#  * a write must require the write source access, which the underlying `StorageFile` does not
#    check on its own.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
DATA_FILE="${CLICKHOUSE_USER_FILES_UNIQUE}/data.csv"
printf '1\n' > "${DATA_FILE}"
chmod 666 "${DATA_FILE}"

DB="db_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Note: the source grants are given one by one, and the write is only exercised as "no source grant"
# vs. "the source is granted". A `READ`-only source grant cannot be relied upon here: with
# `access_control_improvements.enable_read_write_grants` disabled (the default), `GRANT READ ON FILE`
# is stored as the old-style whole-source `FILE` grant, which allows writing as well.
${CLICKHOUSE_CLIENT} -q "
DROP DATABASE IF EXISTS ${DB};
DROP USER IF EXISTS ${USER};
CREATE DATABASE ${DB} ENGINE = Filesystem('${CLICKHOUSE_USER_FILES_UNIQUE}');
CREATE USER ${USER} IDENTIFIED WITH no_password;
GRANT SELECT, INSERT ON ${DB}.* TO ${USER};
"

echo '--- EXISTS must not distinguish an existing file from a missing one without the read source grant'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "EXISTS TABLE ${DB}.\`data.csv\`"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "EXISTS TABLE ${DB}.\`missing.csv\`"

echo '--- SELECT without the read source grant must fail with the same error for both'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`data.csv\`" 2>&1 | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`missing.csv\`" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo '--- a cache entry warmed by a privileged query must not become readable'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB}.\`data.csv\`"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`data.csv\`" 2>&1 | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "EXISTS TABLE ${DB}.\`missing.csv\`"

echo '--- INSERT without the write source grant must fail'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "INSERT INTO ${DB}.\`data.csv\` VALUES (2)" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo '--- INSERT without the write source grant must fail with an asynchronous insert too'
# The sink of an asynchronous insert is created in a background flush, so a check done only when
# the sink is created would neither reach the user (the query has already returned success with
# `wait_for_async_insert = 0`) nor run with the privileges the user had when the query was issued.
${CLICKHOUSE_CLIENT} --user "${USER}" --async_insert 1 --wait_for_async_insert 0 -q "INSERT INTO ${DB}.\`data.csv\` VALUES (2)" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo '--- SELECT with the read source grant'
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO ${USER}"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`data.csv\`"

echo '--- EXISTS answers the file system again with the read source grant'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "EXISTS TABLE ${DB}.\`data.csv\`"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "EXISTS TABLE ${DB}.\`missing.csv\`"

echo '--- INSERT with the write source grant'
${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON FILE TO ${USER}"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "INSERT INTO ${DB}.\`data.csv\` VALUES (2)"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`data.csv\` ORDER BY ALL"

${CLICKHOUSE_CLIENT} -q "
DROP USER ${USER};
DROP DATABASE ${DB};
"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"

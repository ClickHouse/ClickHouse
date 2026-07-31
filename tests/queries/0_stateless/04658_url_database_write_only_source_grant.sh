#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-replicated-database: with `access_control_improvements.enable_read_write_grants` disabled (the
# default) a replicated access storage round-trips the user through serialized SQL, which rewrites
# `WRITE ON FILE` into the old-style whole-source `FILE` grant and allows reading as well.

# An INSERT into a table of a URL database must follow the same source-grant rule as the delegated
# table function: `INSERT INTO FUNCTION file(...)` needs only `WRITE ON FILE`, without `READ`.
# Table resolution must not require the READ source grant for a write-only user, while SELECT must
# still require it.
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

${CLICKHOUSE_CLIENT} -q "
DROP DATABASE IF EXISTS ${DB};
DROP USER IF EXISTS ${USER};
CREATE DATABASE ${DB} ENGINE = URL('file://');
CREATE USER ${USER} IDENTIFIED WITH no_password;
GRANT SELECT, INSERT ON ${DB}.* TO ${USER};
GRANT WRITE ON FILE TO ${USER};
"

echo '--- INSERT with only the write source grant must work'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "INSERT INTO ${DB}.\`${TABLE}\` VALUES (2)"
sort "${DATA_FILE}"

echo '--- SELECT with only the write source grant must still require the read source grant'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`${TABLE}\`" 2>&1 | grep -o -m1 'grant READ ON FILE'

echo '--- SELECT works after granting the read source grant'
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO ${USER}"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`${TABLE}\` ORDER BY ALL"

${CLICKHOUSE_CLIENT} -q "
DROP USER ${USER};
DROP DATABASE ${DB};
"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"

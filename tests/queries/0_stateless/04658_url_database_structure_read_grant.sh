#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-replicated-database: with `access_control_improvements.enable_read_write_grants` disabled (the
# default) a replicated access storage round-trips the user through serialized SQL, which rewrites
# `WRITE ON FILE` into the old-style whole-source `FILE` grant and allows reading as well.

# The structure of a table of a URL database is inferred from the data of the source, and the
# catalog then exposes that structure to the user (`DESCRIBE`, `EXPLAIN`) without ever reading the
# data through the storage. Resolving such a table therefore requires the read source grant, even
# for a user who is allowed to write to the source.
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
GRANT SELECT, INSERT, SHOW COLUMNS ON ${DB}.* TO ${USER};
GRANT WRITE ON FILE TO ${USER};
"

echo '--- DESCRIBE with only the write source grant must fail'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "DESCRIBE TABLE ${DB}.\`${TABLE}\`" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo '--- EXPLAIN with only the write source grant must fail'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "EXPLAIN QUERY TREE SELECT * FROM ${DB}.\`${TABLE}\`" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo '--- SELECT with only the write source grant must fail'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`${TABLE}\`" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo '--- INSERT with only the write source grant must fail'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "INSERT INTO ${DB}.\`${TABLE}\` VALUES (2)" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo '--- everything works after granting the read source grant'
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO ${USER}"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "DESCRIBE TABLE ${DB}.\`${TABLE}\`" | cut -f1,2
${CLICKHOUSE_CLIENT} --user "${USER}" -q "INSERT INTO ${DB}.\`${TABLE}\` VALUES (2)"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`${TABLE}\` ORDER BY ALL"

${CLICKHOUSE_CLIENT} -q "
DROP USER ${USER};
DROP DATABASE ${DB};
"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"

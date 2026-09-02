#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-replicated-database: with `access_control_improvements.enable_read_write_grants` disabled (the
# default) a replicated access storage round-trips the user through serialized SQL, which rewrites
# `READ ON FILE` into the old-style whole-source `FILE` grant.

# `EXISTS TABLE` requires only the `SHOW TABLES` privilege, and resolving a table of a URL database
# reports `FILE_DOESNT_EXIST` before the source access is checked. Without the read source grant,
# neither of them may probe the filesystem: otherwise a URL database is an oracle for the contents
# of `user_files`.
# https://github.com/ClickHouse/ClickHouse/pull/111512

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
DATA_FILE="${CLICKHOUSE_USER_FILES_UNIQUE}/data.csv"
printf '1\n' > "${DATA_FILE}"
chmod 666 "${DATA_FILE}"
MISSING_FILE="${CLICKHOUSE_USER_FILES_UNIQUE}/no_such_file.csv"

DB="db_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "
DROP DATABASE IF EXISTS ${DB};
DROP USER IF EXISTS ${USER};
CREATE DATABASE ${DB} ENGINE = URL('file://');
CREATE USER ${USER} IDENTIFIED WITH no_password;
GRANT SELECT, SHOW TABLES, SHOW COLUMNS ON ${DB}.* TO ${USER};
"

echo '--- without the read source grant, EXISTS must not tell an existing file from a missing one'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "EXISTS TABLE ${DB}.\`${DATA_FILE}\`"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "EXISTS TABLE ${DB}.\`${MISSING_FILE}\`"

echo '--- a missing file is reported as an access error, not as a missing file'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "DESCRIBE TABLE ${DB}.\`${MISSING_FILE}\`" 2>&1 | grep -o -m1 -e 'ACCESS_DENIED' -e 'FILE_DOESNT_EXIST'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`${MISSING_FILE}\`" 2>&1 | grep -o -m1 -e 'ACCESS_DENIED' -e 'FILE_DOESNT_EXIST'

echo '--- without the read source grant, resolution must not parse the delegate arguments (the parse errors would leak path policy before the grant is checked)'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "DESCRIBE TABLE ${DB}.\`/etc/hosts\`" 2>&1 | grep -o -m1 -e 'PATH_ACCESS_DENIED' -e 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`${CLICKHOUSE_USER_FILES_UNIQUE}/*.csv\`" 2>&1 | grep -o -m1 -e 'PATH_ACCESS_DENIED' -e 'ACCESS_DENIED'

echo '--- with the read source grant, EXISTS answers the actual state of the file'
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO ${USER}"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "EXISTS TABLE ${DB}.\`${DATA_FILE}\`"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "EXISTS TABLE ${DB}.\`${MISSING_FILE}\`"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`${DATA_FILE}\`"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${DB}.\`${MISSING_FILE}\`" 2>&1 | grep -o -m1 -e 'ACCESS_DENIED' -e 'UNKNOWN_TABLE'
${CLICKHOUSE_CLIENT} --user "${USER}" -q "DESCRIBE TABLE ${DB}.\`${MISSING_FILE}\`" 2>&1 | grep -o -m1 -e 'ACCESS_DENIED' -e 'FILE_DOESNT_EXIST'

${CLICKHOUSE_CLIENT} -q "
DROP USER ${USER};
DROP DATABASE ${DB};
"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"

#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An INSERT into an attached SQLite table whose database file has gone missing must fail closed: the
# storage never creates the remote table or schema itself, so materializing a brand-new empty database
# file could not satisfy the insert anyway and would only leave junk behind.

DB_PATH="${CLICKHOUSE_TMP}/04642_sqlite_missing.db"
STATE_PATH="${CLICKHOUSE_TMP}/04642_local_state"
rm -rf "${DB_PATH}" "${STATE_PATH}"

sqlite3 "${DB_PATH}" "CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1);"

${CLICKHOUSE_LOCAL} --path "${STATE_PATH}" --query "
    CREATE DATABASE db_04642 ENGINE = Atomic;
    CREATE TABLE db_04642.ch (x Int64) ENGINE = SQLite('${DB_PATH}', 't');
    SELECT * FROM db_04642.ch;
"

rm "${DB_PATH}"

echo "INSERT into a missing SQLite file fails and reports the error code:"
${CLICKHOUSE_LOCAL} --path "${STATE_PATH}" --query "INSERT INTO db_04642.ch VALUES (2)" 2>&1 \
    | grep -o "PATH_ACCESS_DENIED" | head -1

if [ -e "${DB_PATH}" ]; then
    echo "FAIL: the INSERT materialized a missing SQLite database file"
else
    echo "The missing database file was not created"
fi

rm -rf "${DB_PATH}" "${STATE_PATH}"

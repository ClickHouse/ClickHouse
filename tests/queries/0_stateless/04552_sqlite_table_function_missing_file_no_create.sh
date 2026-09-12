#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

# A table function always refers to an already-existing table, so a missing database path can only be a
# mistake. An `INSERT INTO TABLE FUNCTION sqlite('missing.db', ...)` must therefore fail closed instead of
# fabricating a brand-new empty database file that is then left behind as junk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_tf_missing.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

echo 'INSERT into a missing database through the table function fails:'
${CLICKHOUSE_LOCAL} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 'remote_table') VALUES (1)" 2>&1 | grep -o -m1 'PATH_ACCESS_DENIED'

echo 'The database file was not created:'
if [ -f "${DB_PATH}" ]; then echo 'FILE EXISTS'; else echo 'no file'; fi

echo 'SELECT from a missing database through the table function fails too:'
${CLICKHOUSE_LOCAL} --query="SELECT * FROM sqlite('${DB_PATH}', 'remote_table')" 2>&1 | grep -o -m1 'PATH_ACCESS_DENIED'

echo 'Still no file:'
if [ -f "${DB_PATH}" ]; then echo 'FILE EXISTS'; else echo 'no file'; fi

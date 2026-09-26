#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_uuid.db

rm -f "${DB_PATH}"

# Create SQLite database with TEXT column containing UUID values
sqlite3 "${DB_PATH}" 'CREATE TABLE t0 (c0 TEXT);'
sqlite3 "${DB_PATH}" "INSERT INTO t0 VALUES ('f5c9d035-cb46-9811-92c1-169d868ba2db');"
sqlite3 "${DB_PATH}" "INSERT INTO t0 VALUES ('00000000-0000-0000-0000-000000000000');"
sqlite3 "${DB_PATH}" "INSERT INTO t0 VALUES (NULL);"

echo "Test UUID column type"
${CLICKHOUSE_LOCAL} --query="CREATE TABLE test_sqlite_uuid (c0 UUID) ENGINE = SQLite('${DB_PATH}', 't0'); SELECT c0 FROM test_sqlite_uuid ORDER BY c0"

echo "Test Nullable(UUID) column type"
${CLICKHOUSE_LOCAL} --query="CREATE TABLE test_sqlite_uuid_nullable (c0 Nullable(UUID)) ENGINE = SQLite('${DB_PATH}', 't0'); SELECT c0 FROM test_sqlite_uuid_nullable ORDER BY c0 NULLS LAST"

echo "Test inserting UUID values into SQLite"
${CLICKHOUSE_LOCAL} --query="INSERT INTO TABLE FUNCTION sqlite('${DB_PATH}', 't0') SELECT 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'"
${CLICKHOUSE_LOCAL} --query="SELECT c0 FROM sqlite('${DB_PATH}', 't0') ORDER BY c0"

rm -f "${DB_PATH}"

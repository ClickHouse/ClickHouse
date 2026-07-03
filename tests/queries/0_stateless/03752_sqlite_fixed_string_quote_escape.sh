#!/usr/bin/env bash
# Tags: no-fasttest

# Test for https://github.com/ClickHouse/ClickHouse/issues/73519
# FixedString was not properly escaping single quotes for SQLite,
# using backslash escaping (\') instead of quote doubling ('')

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_fixed_string.db"

rm -f "${DB_PATH}"

# Create SQLite table
sqlite3 "${DB_PATH}" 'CREATE TABLE t(c0 TEXT);'

# Create ClickHouse table with FixedString type backed by SQLite
# Insert single quote - this was failing before the fix with:
# "Failed to execute sqlite INSERT query. Status: 1. Message: unrecognized token"
${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE t(c0 FixedString(8)) ENGINE = SQLite('${DB_PATH}', 't');
    INSERT INTO t VALUES ('''');
    INSERT INTO t VALUES ('test''s');
"

# Verify data was inserted by reading directly from SQLite
# (Reading via ClickHouse has a separate bug with FixedString/SQLite)
echo "Data in SQLite (first char hex):"
sqlite3 "${DB_PATH}" "SELECT hex(substr(c0, 1, 1)) FROM t ORDER BY c0;"

echo "Data in SQLite (raw, first 7 chars):"
sqlite3 "${DB_PATH}" "SELECT substr(c0, 1, 7) FROM t ORDER BY c0;"

rm -f "${DB_PATH}"

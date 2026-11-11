#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH=$(mktemp "$CLICKHOUSE_TMP/sqlite_storage_chunk_atomicity_XXXXXX.sqlite")
trap 'rm -f "$DB_PATH"' EXIT

sqlite3 "$DB_PATH" '
    CREATE TABLE engine_table(x INTEGER UNIQUE);
    CREATE TABLE function_table(x INTEGER UNIQUE);
'

DDL="CREATE TABLE local_table ENGINE = SQLite('$DB_PATH', 'engine_table')"

echo 'A constraint violation rolls back the storage engine chunk:'
${CLICKHOUSE_LOCAL} --multiquery --query "
    ${DDL};
    INSERT INTO local_table VALUES (1), (2), (2);
" 2>&1 | grep -oF -m1 'UNIQUE constraint failed'
sqlite3 "$DB_PATH" 'SELECT count() FROM engine_table;'

echo 'A valid storage engine chunk is committed:'
${CLICKHOUSE_LOCAL} --multiquery --query "
    ${DDL};
    INSERT INTO local_table VALUES (1), (2), (3);
"
sqlite3 "$DB_PATH" "SELECT group_concat(x, ',') FROM (SELECT x FROM engine_table ORDER BY x);"

echo 'A constraint violation rolls back the table function chunk:'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO TABLE FUNCTION sqlite('$DB_PATH', 'function_table') VALUES (4), (5), (5)
" 2>&1 | grep -oF -m1 'UNIQUE constraint failed'
sqlite3 "$DB_PATH" 'SELECT count() FROM function_table;'

echo 'A valid table function chunk is committed:'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO TABLE FUNCTION sqlite('$DB_PATH', 'function_table') VALUES (4), (5), (6)
"
sqlite3 "$DB_PATH" "SELECT group_concat(x, ',') FROM (SELECT x FROM function_table ORDER BY x);"

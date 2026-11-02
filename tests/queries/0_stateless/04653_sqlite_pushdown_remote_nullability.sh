#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_pushdown_nullability.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" "
    CREATE TABLE nullable_t(x INTEGER) STRICT;
    INSERT INTO nullable_t VALUES (NULL), (1);

    CREATE TABLE not_null_t(x INTEGER NOT NULL) STRICT;
    INSERT INTO not_null_t VALUES (1);

    CREATE TABLE primary_key_t(x INTEGER PRIMARY KEY) STRICT;
    INSERT INTO primary_key_t VALUES (1);
"

# A predicate on a nullable remote column mapped to a non-`Nullable` ClickHouse type must stay local.
# Otherwise, SQLite can discard the NULL row before `SQLiteStatementReader` raises the exception that an
# unfiltered read would produce.
${CLICKHOUSE_LOCAL} --query="
    CREATE TABLE nullable_t (x Int64) ENGINE = SQLite('$DB_PATH', 'nullable_t');
    SELECT x FROM nullable_t WHERE x = 1;
" 2>&1 | grep -o "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN" | head -1

# Verify both sides of the gate: a nullable remote column drops the predicate, while `NOT NULL` and primary
# key columns retain it because they guarantee that the non-`Nullable` local type cannot encounter NULL.
{
    ${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
        CREATE TABLE nullable_t (x Int64) ENGINE = SQLite('$DB_PATH', 'nullable_t');
        SELECT x FROM nullable_t WHERE x = 1 FORMAT Null;
    " 2>&1

    ${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
        CREATE TABLE not_null_t (x Int64) ENGINE = SQLite('$DB_PATH', 'not_null_t');
        CREATE TABLE primary_key_t (x Int64) ENGINE = SQLite('$DB_PATH', 'primary_key_t');
        SELECT x FROM not_null_t WHERE x = 1 FORMAT Null;
        SELECT x FROM primary_key_t WHERE x = 1 FORMAT Null;
    " 2>&1
} | grep -oE 'Query: SELECT `x` FROM `(nullable_t|not_null_t|primary_key_t)`( WHERE .*)?$'

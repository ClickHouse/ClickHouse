#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_in_null.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# An `INTEGER` column of a `STRICT` table is otherwise pushdown-safe. However, SQLite follows the SQL
# three-valued logic for set membership while ClickHouse does not, so `NOT IN` - and `IN` under
# `transform_null_in` - must not be evaluated remotely: a row dropped by SQLite never reaches the local
# re-filtering.
sqlite3 "$DB_PATH" "
    CREATE TABLE t(x INTEGER) STRICT;
    INSERT INTO t VALUES (1), (2), (NULL);
"

${CLICKHOUSE_LOCAL} --query="
    CREATE TABLE t (x Nullable(Int64)) ENGINE = SQLite('$DB_PATH', 't');

    SELECT 'NOT IN over a set containing NULL:';
    SELECT x FROM t WHERE x NOT IN (1, NULL) ORDER BY x;

    SELECT 'IN with transform_null_in:';
    SELECT x FROM t WHERE x IN (1, NULL) ORDER BY x SETTINGS transform_null_in = 1;

    SELECT 'NOT IN with transform_null_in:';
    SELECT x FROM t WHERE x NOT IN (1, 2) ORDER BY x SETTINGS transform_null_in = 1;

    SELECT 'IN without transform_null_in:';
    SELECT x FROM t WHERE x IN (1, NULL) ORDER BY x;
"

# `NOT IN` always stays local, `IN` only under `transform_null_in`; other predicates still reach SQLite.
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
    CREATE TABLE t (x Nullable(Int64)) ENGINE = SQLite('$DB_PATH', 't');
    SELECT x FROM t WHERE x NOT IN (1, 2) FORMAT Null;
    SELECT x FROM t WHERE x IN (1, 2) FORMAT Null SETTINGS transform_null_in = 1;
    SELECT x FROM t WHERE x IN (1, 2) FORMAT Null;
    SELECT x FROM t WHERE x = 2 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT `x` FROM `t`( WHERE .*)?$'

#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_in_pushdown.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# An `INTEGER` column of a `STRICT` table is otherwise pushdown-safe. However, SQL `IN` uses three-valued
# logic: `NULL IN (1, NULL)`, `NULL NOT IN (1, 2)` and `2 NOT IN (1, NULL)` all evaluate to NULL in SQLite
# and the rows are dropped remotely, while ClickHouse keeps them (the first two with `transform_null_in = 1`,
# the last with any value of the setting). Such predicates must stay local; rows dropped remotely cannot be
# restored by the local re-filter.
sqlite3 "$DB_PATH" "
    CREATE TABLE t(x INTEGER) STRICT;
    INSERT INTO t VALUES (1), (2), (NULL);
"

for enable_analyzer in 0 1; do
${CLICKHOUSE_LOCAL} --enable_analyzer="$enable_analyzer" --query="
    CREATE TABLE t (x Nullable(Int64)) ENGINE = SQLite('$DB_PATH', 't');

    SELECT 'enable_analyzer = $enable_analyzer';

    SELECT 'IN with a NULL element keeps the NULL row under transform_null_in = 1:';
    SELECT x FROM t WHERE x IN (1, NULL) ORDER BY x SETTINGS transform_null_in = 1;

    SELECT 'NOT IN keeps the NULL row under transform_null_in = 1:';
    SELECT x FROM t WHERE x NOT IN (1) ORDER BY x SETTINGS transform_null_in = 1;

    SELECT 'NOT IN with a NULL element keeps non-matching rows under transform_null_in = 0:';
    SELECT x FROM t WHERE x NOT IN (1, NULL) ORDER BY x SETTINGS transform_null_in = 0;

    SELECT 'IN with a NULL element under transform_null_in = 0:';
    SELECT x FROM t WHERE x IN (1, NULL) ORDER BY x SETTINGS transform_null_in = 0;
"
done

# Prove that the diverging `IN` predicates stay local while a NULL-free `IN` under `transform_null_in = 0`
# still reaches SQLite.
for enable_analyzer in 0 1; do
${CLICKHOUSE_LOCAL} --enable_analyzer="$enable_analyzer" --send_logs_level=trace --query="
    CREATE TABLE t (x Nullable(Int64)) ENGINE = SQLite('$DB_PATH', 't');
    SELECT x FROM t WHERE x IN (1, 2) SETTINGS transform_null_in = 1 FORMAT Null;
    SELECT x FROM t WHERE x NOT IN (1) SETTINGS transform_null_in = 1 FORMAT Null;
    SELECT x FROM t WHERE x IN (1, NULL) SETTINGS transform_null_in = 0 FORMAT Null;
    SELECT x FROM t WHERE x NOT IN (1, NULL) SETTINGS transform_null_in = 0 FORMAT Null;
    SELECT x FROM t WHERE x IN (1, 2) SETTINGS transform_null_in = 0 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT "x" FROM "t"( WHERE .*)?$'
done

#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_null_in_pushdown.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# SQLite - like every SQL database - evaluates `IN`/`NOT IN` with three-valued logic: a `NULL` taking
# part in the membership test makes the result `NULL`, and `WHERE` drops the row - unrecoverably,
# before the ClickHouse-side re-filtering ever sees it. ClickHouse disagrees in three cases, so those
# predicates must stay local:
#   - `IN (..., NULL)` with `transform_null_in = 1` keeps the rows whose value is `NULL`;
#   - `NOT IN (..., NULL)` ignores the `NULL` element and keeps the non-members;
#   - `NOT IN (...)` on a nullable column with `transform_null_in = 1` keeps the `NULL` rows.
sqlite3 "$DB_PATH" "
    CREATE TABLE t(x INTEGER) STRICT;
    INSERT INTO t VALUES (1), (2), (NULL);
"

${CLICKHOUSE_LOCAL} --query="
    CREATE TABLE t (x Nullable(Int64)) ENGINE = SQLite('$DB_PATH', 't');

    SELECT 'IN with NULL in the set keeps the NULL row under transform_null_in = 1:';
    SELECT x FROM t WHERE x IN (1, NULL) ORDER BY x NULLS LAST SETTINGS transform_null_in = 1;

    SELECT 'NOT IN with NULL in the set keeps the non-members:';
    SELECT x FROM t WHERE x NOT IN (1, NULL) ORDER BY x NULLS LAST SETTINGS transform_null_in = 0;

    SELECT 'NOT IN on a nullable column keeps the NULL row under transform_null_in = 1:';
    SELECT x FROM t WHERE x NOT IN (1, 2) ORDER BY x NULLS LAST SETTINGS transform_null_in = 1;

    SELECT 'IN without NULL in the set under transform_null_in = 0:';
    SELECT x FROM t WHERE x IN (1, 2) ORDER BY x NULLS LAST SETTINGS transform_null_in = 0;

    SELECT 'NOT IN without NULL in the set under transform_null_in = 0:';
    SELECT x FROM t WHERE x NOT IN (1, 2) ORDER BY x NULLS LAST SETTINGS transform_null_in = 0;
"

# The predicates whose two sides agree are still pushed down; the three unsafe shapes stay local.
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
    CREATE TABLE t (x Nullable(Int64)) ENGINE = SQLite('$DB_PATH', 't');
    SELECT x FROM t WHERE x IN (1, 2) FORMAT Null SETTINGS transform_null_in = 0;
    SELECT x FROM t WHERE x IN (1, 2) FORMAT Null SETTINGS transform_null_in = 1;
    SELECT x FROM t WHERE x NOT IN (1, 2) FORMAT Null SETTINGS transform_null_in = 0;
    SELECT x FROM t WHERE x IN (1, NULL) FORMAT Null SETTINGS transform_null_in = 1;
    SELECT x FROM t WHERE x NOT IN (1, NULL) FORMAT Null SETTINGS transform_null_in = 0;
    SELECT x FROM t WHERE x NOT IN (1, 2) FORMAT Null SETTINGS transform_null_in = 1;
" 2>&1 | grep -oE 'Query: SELECT "x" FROM "t"( WHERE .*)?$'

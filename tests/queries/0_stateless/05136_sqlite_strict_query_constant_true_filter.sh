#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_strict_constant_true.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" "
CREATE TABLE t(i INTEGER NOT NULL) STRICT;
INSERT INTO t VALUES (1), (2);
"

# A surviving outer predicate that references no column of the query-backed source is not a filter on
# the source, so `external_table_strict_query` must let it through - the same way a bare `WHERE 1` is
# already let through. Only a predicate over the source's own columns is rejected.
for analyzer in 1 0
do
    echo "enable_analyzer = ${analyzer}"

    ${CLICKHOUSE_LOCAL} --multiquery --query="
    CREATE TABLE query_ext ENGINE = SQLite('${DB_PATH}', query('SELECT i FROM t'));

    SELECT 'literal one', count() FROM query_ext WHERE 1
        SETTINGS external_table_strict_query = 1, enable_analyzer = ${analyzer};

    SELECT 'constant equality', count() FROM query_ext WHERE 1 = 1
        SETTINGS external_table_strict_query = 1, enable_analyzer = ${analyzer};

    SELECT 'constant conjunction', count() FROM query_ext WHERE 1 = 1 AND 2 > 1
        SETTINGS external_table_strict_query = 1, enable_analyzer = ${analyzer};
    "

    echo -n 'filter on a source column is still rejected: '
    ${CLICKHOUSE_LOCAL} --multiquery --query="
    CREATE TABLE query_ext ENGINE = SQLite('${DB_PATH}', query('SELECT i FROM t'));
    SELECT count() FROM query_ext WHERE i = 1
        SETTINGS external_table_strict_query = 1, enable_analyzer = ${analyzer};
    " 2>&1 | grep -c 'INCORRECT_QUERY'
done

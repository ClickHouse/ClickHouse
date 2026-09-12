#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_strict_materialized.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" "
CREATE TABLE t(i INTEGER NOT NULL, m INTEGER NOT NULL) STRICT;
INSERT INTO t VALUES (1, 2), (2, 3);
"

# `MATERIALIZED` and `ALIAS` columns belong to this source, but are not part of the set of columns whose
# predicates are pushed down to the external database: a filter over one of them is applied locally. Such
# a filter must be rejected under `external_table_strict_query` instead of being silently dropped as if it
# belonged to another table. A filter over an ordinary column is still pushed down and accepted.
for analyzer in 1 0
do
    echo "enable_analyzer = ${analyzer}"

    ${CLICKHOUSE_LOCAL} --multiquery --query="
    CREATE TABLE ext (i Int64, m Int64 MATERIALIZED i + 1, a Int64 ALIAS i * 10) ENGINE = SQLite('${DB_PATH}', 't');

    SELECT 'materialized filter, default', count() FROM ext WHERE m = 2
        SETTINGS enable_analyzer = ${analyzer};

    SELECT 'alias filter, default', count() FROM ext WHERE a = 10
        SETTINGS enable_analyzer = ${analyzer};

    SELECT 'ordinary filter, strict', count() FROM ext WHERE i = 1
        SETTINGS external_table_strict_query = 1, enable_analyzer = ${analyzer};
    "

    echo -n 'materialized filter, strict: '
    ${CLICKHOUSE_LOCAL} --multiquery --query="
    CREATE TABLE ext (i Int64, m Int64 MATERIALIZED i + 1) ENGINE = SQLite('${DB_PATH}', 't');
    SELECT count() FROM ext WHERE m = 2
        SETTINGS external_table_strict_query = 1, enable_analyzer = ${analyzer};
    " 2>&1 | grep -c 'INCORRECT_QUERY'

    echo -n 'alias filter, strict: '
    ${CLICKHOUSE_LOCAL} --multiquery --query="
    CREATE TABLE ext (i Int64, a Int64 ALIAS i * 10) ENGINE = SQLite('${DB_PATH}', 't');
    SELECT count() FROM ext WHERE a = 10
        SETTINGS external_table_strict_query = 1, enable_analyzer = ${analyzer};
    " 2>&1 | grep -c 'INCORRECT_QUERY'
done

#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_strict_generated.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" "
CREATE TABLE t(i INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (i + 1) STORED) STRICT;
INSERT INTO t(i) VALUES (1), (2);
"

# A SQLite `GENERATED ALWAYS AS` column is kept in the table structure as an expressionless `MATERIALIZED`
# column: the marker only makes it non-insertable, its value is read from SQLite like that of any ordinary
# physical column. So it must stay pushdown-eligible - a filter over it belongs to the remote query, and
# `external_table_strict_query = 1` must accept it - unlike a `MATERIALIZED` column with a local expression,
# whose value ClickHouse computes itself (see `05182_sqlite_strict_query_materialized_column`).
for analyzer in 1 0
do
    echo "enable_analyzer = ${analyzer}"

    ${CLICKHOUSE_LOCAL} --multiquery --query="
    CREATE TABLE ext (i Int64, g Int64) ENGINE = SQLite('${DB_PATH}', 't');

    SELECT 'structure', name, type, default_kind FROM system.columns WHERE database = currentDatabase() AND table = 'ext' ORDER BY name;

    SELECT 'generated filter, default', count() FROM ext WHERE g = 2
        SETTINGS enable_analyzer = ${analyzer};

    SELECT 'generated filter, strict', count() FROM ext WHERE g = 2
        SETTINGS external_table_strict_query = 1, enable_analyzer = ${analyzer};

    SELECT 'generated column values', i, g FROM ext ORDER BY i
        SETTINGS enable_analyzer = ${analyzer};
    "
done

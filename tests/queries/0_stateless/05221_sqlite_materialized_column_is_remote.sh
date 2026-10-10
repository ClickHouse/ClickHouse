#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_materialized_remote.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# `t` has the column `m`, `s` does not.
sqlite3 "$DB_PATH" "
CREATE TABLE t(a INTEGER NOT NULL, m INTEGER NOT NULL) STRICT;
INSERT INTO t VALUES (1, 100), (2, 200);
CREATE TABLE s(a INTEGER NOT NULL) STRICT;
INSERT INTO s VALUES (1), (2);
"

# A `MATERIALIZED` column of an external table is a physical column of the remote source, exactly like a
# `MATERIALIZED` column of a `MergeTree` table is stored on disk: its expression is evaluated on `INSERT`, and
# a read takes the stored value from the source instead of re-evaluating the expression. So the column is
# projected from the source, a predicate over it is pushed down and `external_table_strict_query` accepts it,
# and when the source does not provide the column the read fails instead of silently computing it locally.
# An `ALIAS` column is the local counterpart: it is computed from its expression on every read.
${CLICKHOUSE_LOCAL} --multiquery --query="
CREATE TABLE ext (a Int64, m Int64 MATERIALIZED a + 1, l Int64 ALIAS a * 10) ENGINE = SQLite('${DB_PATH}', 't');

SELECT 'table source, values', a, m, l FROM ext ORDER BY a;

SELECT 'table source, materialized filter, strict', count() FROM ext WHERE m = 100
    SETTINGS external_table_strict_query = 1;

CREATE TABLE ext_query (a Int64, m Int64 MATERIALIZED a + 1, l Int64 ALIAS a * 10)
    ENGINE = SQLite('${DB_PATH}', query('SELECT a, m FROM t'));

SELECT 'query source, values', a, m, l FROM ext_query ORDER BY a;

CREATE TABLE ext_query_without_m (a Int64, m Int64 MATERIALIZED a + 1, l Int64 ALIAS a * 10)
    ENGINE = SQLite('${DB_PATH}', query('SELECT a FROM s'));

SELECT 'query source without the column, alias', a, l FROM ext_query_without_m ORDER BY a;

SELECT 'query source without the column, materialized', m FROM ext_query_without_m; -- { serverError SQLITE_ENGINE_ERROR }

CREATE TABLE ext_table_without_m (a Int64, m Int64 MATERIALIZED a + 1) ENGINE = SQLite('${DB_PATH}', 's');

SELECT 'table source without the column, materialized', m FROM ext_table_without_m; -- { serverError SQLITE_ENGINE_ERROR }
"

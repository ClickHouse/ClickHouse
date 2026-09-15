#!/usr/bin/env bash
# Resolving a table puts its columns into the query tree, and `EXPLAIN QUERY TREE` returns that tree
# without ever planning the query, so the planner's access checks are not reached.
# Behind a read-only `Overlay` facade the resolved metadata is the source table's, and the facade
# must not widen access: at least one column has to be visible on the source too, not only on the
# facade name.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_SRC="db_src_${CLICKHOUSE_DATABASE}"
DB_OVL="db_ovl_${CLICKHOUSE_DATABASE}"
USER_OVL="u_ovl_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_OVL};
DROP DATABASE IF EXISTS ${DB_SRC};
DROP USER IF EXISTS ${USER_OVL};

CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
CREATE TABLE ${DB_SRC}.t (a UInt8, secret_column UInt8) ENGINE = MergeTree ORDER BY a;
CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

CREATE USER ${USER_OVL} NOT IDENTIFIED;
-- Granted on the facade only. \`SHOW TABLES\` on the source lets the name resolve at all, but says
-- nothing about the source's columns.
GRANT SHOW TABLES ON ${DB_SRC}.* TO ${USER_OVL};
GRANT SELECT(a) ON ${DB_OVL}.t TO ${USER_OVL};
"

explain()
{
    $CLICKHOUSE_CLIENT --user "${USER_OVL}" --enable_analyzer 1 -q "$1" 2>&1 \
        | grep -oE 'ACCESS_DENIED' | sort -u | tr '\n' ' '
    echo
}

echo 'granted on the facade alone, the source metadata stays hidden'
explain "EXPLAIN QUERY TREE SELECT secret_column FROM ${DB_OVL}.t"
explain "EXPLAIN QUERY TREE SELECT a FROM ${DB_OVL}.t"

echo 'with the source-side grant the analysis works again'
$CLICKHOUSE_CLIENT -q "GRANT SELECT(a) ON ${DB_SRC}.t TO ${USER_OVL}"
explain "EXPLAIN QUERY TREE SELECT a FROM ${DB_OVL}.t"
echo -n 'and reading the granted column works: '
$CLICKHOUSE_CLIENT --user "${USER_OVL}" --enable_analyzer 1 -q "SELECT count() FROM ${DB_OVL}.t"

$CLICKHOUSE_CLIENT -m -q "
DROP USER ${USER_OVL};
DROP DATABASE ${DB_OVL};
DROP DATABASE ${DB_SRC};
"

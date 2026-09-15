#!/usr/bin/env bash
# With `analyzer_inline_views = 1` an ordinary view is expanded into its body, which removes the
# `TableNode` the planner would otherwise run its `SELECT` check against. Reading a view through a
# read-only `Overlay` facade must require the grant on the facade *and* on the underlying source
# regardless of that setting, so inlining is gated on the grant through every id the read needs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_SRC="db_src_${CLICKHOUSE_DATABASE}"
DB_OVL="db_ovl_${CLICKHOUSE_DATABASE}"
USER_SRC="u_src_${CLICKHOUSE_DATABASE}"
USER_OVL="u_ovl_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_OVL};
DROP DATABASE IF EXISTS ${DB_SRC};
DROP USER IF EXISTS ${USER_SRC};
DROP USER IF EXISTS ${USER_OVL};

CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
CREATE TABLE ${DB_SRC}.t (a UInt8, b UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO ${DB_SRC}.t VALUES (1, 10), (2, 20);
CREATE VIEW ${DB_SRC}.v AS SELECT a, b FROM ${DB_SRC}.t;
CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

-- Granted on the source only; on the facade just enough to resolve the name and one column.
CREATE USER ${USER_SRC} NOT IDENTIFIED;
GRANT SELECT(a) ON ${DB_OVL}.v TO ${USER_SRC};
GRANT SELECT ON ${DB_SRC}.* TO ${USER_SRC};

-- Granted on the facade only.
CREATE USER ${USER_OVL} NOT IDENTIFIED;
GRANT SELECT ON ${DB_OVL}.* TO ${USER_OVL};
GRANT SHOW TABLES ON ${DB_SRC}.* TO ${USER_OVL};
"

read_through_facade()
{
    $CLICKHOUSE_CLIENT --user "$1" --enable_analyzer 1 --analyzer_inline_views "$2" \
        -q "SELECT sum(b) FROM ${DB_OVL}.v" 2>&1 | grep -oE 'ACCESS_DENIED|^[0-9]+' | head -1
}

echo 'a source-side grant alone does not read through the facade, inlined or not'
read_through_facade "${USER_SRC}" 0
read_through_facade "${USER_SRC}" 1

echo 'a facade-side grant alone does not either'
read_through_facade "${USER_OVL}" 0
read_through_facade "${USER_OVL}" 1

echo 'reading the source view directly still works for the source-granted user'
$CLICKHOUSE_CLIENT --user "${USER_SRC}" --enable_analyzer 1 --analyzer_inline_views 1 -q "SELECT sum(b) FROM ${DB_SRC}.v"

echo 'with both grants, inlined and non-inlined reads agree'
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${DB_OVL}.* TO ${USER_SRC}"
read_through_facade "${USER_SRC}" 0
read_through_facade "${USER_SRC}" 1

$CLICKHOUSE_CLIENT -m -q "
DROP USER ${USER_SRC};
DROP USER ${USER_OVL};
DROP DATABASE ${DB_OVL};
DROP DATABASE ${DB_SRC};
"

#!/usr/bin/env bash
# `UNDROP TABLE` through a read-only `Overlay` facade must be rejected up front by the database
# name. `InterpreterUndropQuery` used to probe `isTableExist` first, so `UNDROP TABLE ov.<name>`
# answered `TABLE_ALREADY_EXISTS` for an existing source table but reached the facade's
# `TABLE_IS_PERMANENTLY_READ_ONLY` only for a missing one — a facade-scoped `UNDROP_TABLE` grant
# worked as a source-table existence oracle for hidden source tables. Both cases must now fail
# with `TABLE_IS_PERMANENTLY_READ_ONLY`, indistinguishably, and `UNDROP` in the underlying
# database itself must keep working.
#
# The databases are named after `CLICKHOUSE_DATABASE` because they are server-wide objects: with
# fixed names, another run of this same test (the flaky check runs it repeatedly) drops the
# databases from under this one.
# Related: https://github.com/ClickHouse/ClickHouse/pull/86768

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_SRC="db_src_${CLICKHOUSE_DATABASE}"
DB_OVL="db_ovl_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_OVL};
DROP DATABASE IF EXISTS ${DB_SRC};

CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
CREATE TABLE ${DB_SRC}.t_kept (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ${DB_SRC}.t_dropped (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${DB_SRC}.t_dropped VALUES (1), (2);
-- An UNDROP-able table must still be in the delayed-drop queue, not dropped synchronously.
SET database_atomic_wait_for_drop_and_detach_synchronously = 0;
DROP TABLE ${DB_SRC}.t_dropped;

CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');
"

echo "-- UNDROP of an existing source table through the facade: rejected, not TABLE_ALREADY_EXISTS"
$CLICKHOUSE_CLIENT -q "UNDROP TABLE ${DB_OVL}.t_kept" 2>&1 | grep -oF 'TABLE_IS_PERMANENTLY_READ_ONLY' | head -1

echo "-- UNDROP of a missing source table through the facade: the same error (no existence oracle)"
$CLICKHOUSE_CLIENT -q "UNDROP TABLE ${DB_OVL}.no_such_table" 2>&1 | grep -oF 'TABLE_IS_PERMANENTLY_READ_ONLY' | head -1

echo "-- UNDROP of a genuinely dropped source table through the facade: still the same error"
$CLICKHOUSE_CLIENT -q "UNDROP TABLE ${DB_OVL}.t_dropped" 2>&1 | grep -oF 'TABLE_IS_PERMANENTLY_READ_ONLY' | head -1

echo "-- UNDROP in the underlying database keeps working"
$CLICKHOUSE_CLIENT -m -q "
UNDROP TABLE ${DB_SRC}.t_dropped;
SELECT count() FROM ${DB_SRC}.t_dropped;
"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE ${DB_OVL};
DROP DATABASE ${DB_SRC};
"

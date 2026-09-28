#!/usr/bin/env bash
# Reading `system.table_settings` needs a `SELECT` grant on it, as `system.parts`, `system.data_skipping_indices` and
# `system.s3_queue_settings` do - not the implicit access `system.tables` has. A row can say more than the stored
# definition does: values the server's `<merge_tree>` config section set, macros expanded, state held in Keeper, each
# of which another system table keeps behind a grant. `SHOW TABLES` on a table is therefore not enough, and neither
# is it for `SHOW TABLE SETTINGS`, which reads the table - as `SHOW INDEX` needs `system.data_skipping_indices`.
# Which tables a granted reader sees is the second gate, `SHOW TABLES`, which `05137` covers. Each refusal is
# matched by the grant it names, so that an error from anything else - creating the temporary table, say - does
# not pass for it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
USER="${DB}_settings_reader"

# The flaky check runs a test many times against the same database, so it has to be re-runnable.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${DB}.mt"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.mt (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4096"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SHOW TABLES ON ${DB}.mt TO ${USER}"
$CLICKHOUSE_CLIENT -q "GRANT CREATE TEMPORARY TABLE ON *.* TO ${USER}"
# Where `table_engines_require_grant` is on, as in the test configuration, creating one also needs its engine.
$CLICKHOUSE_CLIENT -q "GRANT TABLE ENGINE ON Memory TO ${USER}"

echo "-- SHOW TABLES on the table, and no grant on the system table: both surfaces refuse"
$CLICKHOUSE_CLIENT --user="${USER}" -q "SELECT count() FROM system.table_settings" 2>&1 | grep -o -m1 'ON system.table_settings'
$CLICKHOUSE_CLIENT --user="${USER}" -q "SHOW TABLE SETTINGS FROM ${DB}.mt" 2>&1 | grep -o -m1 'ON system.table_settings'

echo "-- the reader's own temporary table too, since the statement reads the same table"
$CLICKHOUSE_CLIENT --user="${USER}" -n -q "
CREATE TEMPORARY TABLE own_tmp (a UInt64) ENGINE = Memory;
SHOW TABLE SETTINGS FROM own_tmp;" 2>&1 | grep -o -m1 'ON system.table_settings'

echo "-- with the grant, both read the table"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.table_settings TO ${USER}"
$CLICKHOUSE_CLIENT --user="${USER}" -q "
    SELECT name, value, source FROM system.table_settings
    WHERE database = '${DB}' AND table = 'mt' AND name = 'index_granularity'"
$CLICKHOUSE_CLIENT --user="${USER}" -q "SHOW TABLE SETTINGS FROM ${DB}.mt LIKE 'index_granularity'"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.mt"

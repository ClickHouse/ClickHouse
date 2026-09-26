#!/usr/bin/env bash
# The privilege boundary of the two new surfaces.
#
# `system.table_settings` filters rows by `SHOW TABLES`, per database and then per table, the same
# way `system.tables` and `system.columns` do, and `SHOW TABLE SETTINGS` reads that table. Both are
# checked here, on local tables. The statement also enables `show_remote_databases_in_system_tables`
# when it names a remote database; that the grant still decides on that path needs a reachable remote
# database, and `test_mysql_database_engine::test_table_settings_for_mysql_database` covers it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
DENIED="${DB}_denied"
GRANTED="${DB}_granted"

# The flaky check runs a test many times against the same database, so it has to be re-runnable.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${DB}.mt"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${DB}.other"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.mt (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4096"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.other (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192"

for u in "${DENIED}" "${GRANTED}"; do
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${u}"
    $CLICKHOUSE_CLIENT -q "CREATE USER ${u} IDENTIFIED WITH no_password"
done
# The denied user may read the system table itself - the point is that it yields no rows for a
# table it has no SHOW TABLES on, rather than that the query is refused outright.
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.table_settings TO ${DENIED}"
$CLICKHOUSE_CLIENT -q "GRANT SHOW TABLES ON ${DB}.mt TO ${GRANTED}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.table_settings TO ${GRANTED}"

echo "-- without SHOW TABLES, system.table_settings yields nothing for the table"
$CLICKHOUSE_CLIENT --user="${DENIED}" -q \
    "SELECT count() FROM system.table_settings WHERE database = '${DB}' AND table = 'mt'"

echo "-- SHOW TABLE SETTINGS refuses it, as SHOW CREATE TABLE does, rather than print an empty list"
$CLICKHOUSE_CLIENT --user="${DENIED}" -q "SHOW TABLE SETTINGS FROM ${DB}.mt" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo "-- a granted user sees its own table"
$CLICKHOUSE_CLIENT --user="${GRANTED}" -q \
    "SELECT name, value FROM system.table_settings WHERE database = '${DB}' AND table = 'mt' AND name = 'index_granularity'"

echo "-- and SHOW TABLE SETTINGS returns it too"
$CLICKHOUSE_CLIENT --user="${GRANTED}" -q "SHOW TABLE SETTINGS FROM ${DB}.mt LIKE 'index_granularity'"

echo "-- but not the neighbouring table it was not granted"
$CLICKHOUSE_CLIENT --user="${GRANTED}" -q \
    "SELECT count() FROM system.table_settings WHERE database = '${DB}' AND table = 'other'"

echo "-- the per-table grant does not leak the neighbour through the statement either"
$CLICKHOUSE_CLIENT --user="${GRANTED}" -q "SHOW TABLE SETTINGS FROM ${DB}.other" 2>&1 | grep -o -m1 'ACCESS_DENIED'

for u in "${DENIED}" "${GRANTED}"; do
    $CLICKHOUSE_CLIENT -q "DROP USER ${u}"
done
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.mt"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.other"

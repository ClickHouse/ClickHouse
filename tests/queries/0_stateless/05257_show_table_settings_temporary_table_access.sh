#!/usr/bin/env bash
# The one path into `SHOW TABLE SETTINGS` that checks no privilege: a temporary table.
#
# It needs none - a session's temporary tables are its own, and `system.table_settings` enumerates
# them from the session as `system.tables` does - but that makes this the branch where a mistake
# would not be refused, so it is pinned from both sides: that a user with no grant at all reads its
# own temporary table, and that a temporary table of the same name does not become a way to read a
# permanent one the user may not see.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
USER="${DB}_tmp_reader"

# The flaky check runs a test many times against the same database, so it has to be re-runnable.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${DB}.shadowed"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"

# The permanent table the user is never granted. Its `index_granularity` is what would show up if the
# statement ever answered about it.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE ${DB}.shadowed (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4096"

$CLICKHOUSE_CLIENT -q "CREATE USER ${USER} IDENTIFIED WITH no_password"
# Enough to reach the surface and to make a temporary table, and nothing on the permanent table.
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.table_settings TO ${USER}"
$CLICKHOUSE_CLIENT -q "GRANT CREATE TEMPORARY TABLE ON *.* TO ${USER}"

echo "-- its own temporary table, with no grant on any table, reports its definition"
$CLICKHOUSE_CLIENT --user="${USER}" -n -q "
CREATE TEMPORARY TABLE own_tmp (a UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 7;
SHOW TABLE SETTINGS FROM own_tmp LIKE 'max_rows_to_keep';"

echo "-- and the system table agrees, reporting it under the empty database"
$CLICKHOUSE_CLIENT --user="${USER}" -n -q "
CREATE TEMPORARY TABLE own_tmp (a UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 7;
SELECT database, name, value, source FROM system.table_settings
WHERE table = 'own_tmp' AND name = 'max_rows_to_keep';"

echo "-- a temporary table shadowing a permanent one reports the temporary, not the table it hides"
# `index_granularity` belongs to the permanent table and no row of it may appear under this name. A
# `Memory` temporary table has no such setting, so counting it proves which of the two answered - the
# count is over every database, so a row of the hidden table would be caught wherever it was reported.
$CLICKHOUSE_CLIENT --user="${USER}" -n -q "
CREATE TEMPORARY TABLE shadowed (a UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 13;
SHOW TABLE SETTINGS FROM shadowed LIKE 'max_rows_to_keep';
SELECT count() FROM system.table_settings WHERE table = 'shadowed' AND name = 'index_granularity';"

echo "-- and naming the permanent table itself is still refused"
$CLICKHOUSE_CLIENT --user="${USER}" -q \
    "SHOW TABLE SETTINGS FROM ${DB}.shadowed" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo "-- as is reading it through the system table, which yields no row for it"
$CLICKHOUSE_CLIENT --user="${USER}" -q \
    "SELECT count() FROM system.table_settings WHERE database = '${DB}' AND table = 'shadowed'"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.shadowed"

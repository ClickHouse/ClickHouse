#!/usr/bin/env bash
# A global `SHOW COLUMNS` grant must not become a blanket bypass of the per-entry checks of
# `system.columns_cache`: unlike `system.columns`, which exposes schema only, this table is an
# operational surface, so a user holding the grant globally together with an explicit revoke on
# one table - or on one column of it - must still be denied the part names, row ranges and cached
# sizes of what was revoked.
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user_05232_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cc_revoke_visible"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cc_revoke_hidden"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"

for table in t_cc_revoke_visible t_cc_revoke_hidden; do
    $CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${table} (id UInt64, secret String)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0
    "
    $CLICKHOUSE_CLIENT -q "INSERT INTO ${table} SELECT number, toString(number) FROM numbers(10000)"
    # The read has to touch the columns, otherwise nothing is deserialized into the cache.
    $CLICKHOUSE_CLIENT -q "
    SELECT sum(id), max(secret) FROM ${table} SETTINGS use_columns_cache = 1
    " > /dev/null
done

$CLICKHOUSE_CLIENT -q "CREATE USER ${user}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.columns_cache TO ${user}"
# The global grant, and an explicit revoke of one whole table and of one column of the other.
# The revokes are of `SHOW COLUMNS` rather than of `SHOW TABLES`: any grant on a table implies
# showing the table, so revoking only `SHOW TABLES` would leave it derivable from the global
# `SHOW COLUMNS` again, and nothing would be revoked at all.
$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS ON *.* TO ${user}"
$CLICKHOUSE_CLIENT -q "REVOKE SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.t_cc_revoke_hidden FROM ${user}"
$CLICKHOUSE_CLIENT -q "REVOKE SHOW COLUMNS(secret) ON ${CLICKHOUSE_DATABASE}.t_cc_revoke_visible FROM ${user}"

# Only what was not revoked: the other table, and of it only the column that is still granted.
echo 'tables and columns visible with the global grant:'
$CLICKHOUSE_CLIENT --user "${user}" -q "
SELECT table, arraySort(groupUniqArray(column)) FROM system.columns_cache
WHERE database = '${CLICKHOUSE_DATABASE}' AND table LIKE 't_cc_revoke_%'
GROUP BY table ORDER BY table
"

# The privileged user still sees both tables with both columns, so the check above is not
# passing merely because the cache is empty.
echo 'tables and columns visible to the owner:'
$CLICKHOUSE_CLIENT -q "
SELECT table, arraySort(groupUniqArray(column)) FROM system.columns_cache
WHERE database = currentDatabase() AND table LIKE 't_cc_revoke_%'
GROUP BY table ORDER BY table
"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_cc_revoke_visible"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_cc_revoke_hidden"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

#!/usr/bin/env bash
# `system.engine_settings` reports, for the `MergeTree` family, the server-wide baseline `system.merge_tree_settings`
# and `system.replicated_merge_tree_settings` report - the `<merge_tree>` config section and `compatibility`
# applied - and for other engines server-level values of the same kind. So it is read under the same rule as those
# two: a `SELECT` grant on it, which `select_from_system_db_requires_grant` requires, not the implicit access
# `system.tables` has. Without it, a user refused the baseline in one table would read it in the other.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER="${CLICKHOUSE_DATABASE}_engine_settings_reader"

# The flaky check runs a test many times against the same database, so it has to be re-runnable.
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER} IDENTIFIED WITH no_password"

echo "-- without a grant, all three refuse"
for table in merge_tree_settings replicated_merge_tree_settings engine_settings; do
    $CLICKHOUSE_CLIENT --user="${USER}" -q "SELECT count() FROM system.${table}" 2>&1 | grep -o -m1 'ACCESS_DENIED'
done

echo "-- with one, both report the MergeTree baseline, and report it alike"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.merge_tree_settings TO ${USER}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.engine_settings TO ${USER}"
$CLICKHOUSE_CLIENT --user="${USER}" -q "
    SELECT count() FROM (
        SELECT name, value FROM system.engine_settings WHERE engine = 'MergeTree' AND alias_for = ''
        EXCEPT
        SELECT name, value FROM system.merge_tree_settings WHERE alias_for = '')"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"

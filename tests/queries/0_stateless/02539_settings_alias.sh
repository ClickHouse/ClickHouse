#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# always check two values so we don't hit the default and make the test useless

for check_query in "SELECT value FROM system.settings WHERE name = 'alter_sync';" "SELECT getSetting('alter_sync');"; do
    echo "Checking setting value with '$check_query'"

    echo 'Using SET'
    $CLICKHOUSE_CLIENT -m -q """
    SET replication_alter_partitions_sync = 0;
    $check_query

    SET replication_alter_partitions_sync = 2;
    $check_query
    """

    echo 'Using HTTP with query params'
    ${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL&replication_alter_partitions_sync=0" -d "$check_query"
    ${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL&replication_alter_partitions_sync=2" -d "$check_query"

    echo 'Using client options'
    $CLICKHOUSE_CLIENT --replication_alter_partitions_sync=0 -q "$check_query"
    $CLICKHOUSE_CLIENT --replication_alter_partitions_sync=2 -q "$check_query"
done


$CLICKHOUSE_CLIENT -m -q """
DROP VIEW IF EXISTS 02539_settings_alias_view;
CREATE VIEW 02539_settings_alias_view AS SELECT 1 SETTINGS replication_alter_partitions_sync = 2;
SHOW CREATE TABLE 02539_settings_alias_view;
DROP VIEW 02539_settings_alias_view;
"""

for setting_name in "replication_alter_partitions_sync" "alter_sync"; do
    query="SELECT name, value, changed, alias_for FROM system.settings WHERE name = '$setting_name'"
    $CLICKHOUSE_CLIENT --replication_alter_partitions_sync=0 -q "$query"
    $CLICKHOUSE_CLIENT --replication_alter_partitions_sync=2 -q "$query"
done

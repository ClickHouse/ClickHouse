#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

BACKUP="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO src VALUES (111);
"

U=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'src'")

$CLICKHOUSE_CLIENT -q "
    CREATE VIEW v AS SELECT k FROM src UUID '$U';
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 HOUR DEPENDS ON src UUID '$U' APPEND
        ENGINE = MergeTree ORDER BY tuple() AS SELECT 1 AS x;
"

echo '-- the live definitions keep the pin'
$CLICKHOUSE_CLIENT -q "
    SELECT name, position(create_table_query, 'UUID \'$U\'') > 0
    FROM system.tables WHERE database = currentDatabase() AND name IN ('mv', 'v') ORDER BY name;
"

$CLICKHOUSE_CLIENT -q "BACKUP DATABASE ${CLICKHOUSE_DATABASE} TO $BACKUP" > /dev/null

echo '-- backing up does not touch them either'
$CLICKHOUSE_CLIENT -q "
    SELECT name, position(create_table_query, 'UUID \'$U\'') > 0
    FROM system.tables WHERE database = currentDatabase() AND name IN ('mv', 'v') ORDER BY name;
"
$CLICKHOUSE_CLIENT -q "SELECT k FROM v SETTINGS enable_analyzer = 0"

# RESTORE mints a fresh UUID for `src`, so a pin taken from the backup would resolve to nothing.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE v SYNC;
    DROP TABLE mv SYNC;
    DROP TABLE src SYNC;
"
$CLICKHOUSE_CLIENT -q "RESTORE DATABASE ${CLICKHOUSE_DATABASE} FROM $BACKUP" > /dev/null

echo '-- the restored definitions carry no pin'
$CLICKHOUSE_CLIENT -q "
    SELECT name, position(create_table_query, 'UUID \'') > 0
    FROM system.tables WHERE database = currentDatabase() AND name IN ('mv', 'v') ORDER BY name;
"

echo '-- so the restored view reads the restored table under either analyzer'
$CLICKHOUSE_CLIENT -q "SELECT k FROM v SETTINGS enable_analyzer = 0"
$CLICKHOUSE_CLIENT -q "SELECT k FROM v SETTINGS enable_analyzer = 1"

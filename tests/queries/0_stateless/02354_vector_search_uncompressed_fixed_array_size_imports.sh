#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: backups are not configured in the fast test.

# The parts of a table with a column with the setting `uncompressed_fixed_array_size` are read at positions computed from
# the setting, so the operations that bring in parts that the table did not write are not supported: `RESTORE` of data
# and `ATTACH TABLE` with a definition.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS tab;
    CREATE TABLE tab (id UInt32, vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = 2))
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
    INSERT INTO tab SELECT number, [toFloat32(number), number + 1] FROM numbers(10);
"

echo "backup"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE tab TO $backup FORMAT Null"

echo "restore of data"
$CLICKHOUSE_CLIENT -q "DROP TABLE tab SYNC"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}.tab FROM $backup FORMAT Null" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"

echo "restore of the structure"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS tab SYNC"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}.tab FROM $backup SETTINGS structure_only = 1 FORMAT Null"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM tab"

echo "attach with a definition"
uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'tab'")
$CLICKHOUSE_CLIENT -q "DETACH TABLE tab"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE tab UUID '$uuid' (id UInt32, vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = 2))
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE tab"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM tab"

$CLICKHOUSE_CLIENT -q "DROP TABLE tab"

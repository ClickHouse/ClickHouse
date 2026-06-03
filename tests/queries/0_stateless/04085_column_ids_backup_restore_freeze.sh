#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas
# Regression for the BACKUP / RESTORE / FREEZE path: with column IDs active
# any column that has a non-identity column_id (DROP + same-name ADD, RENAME
# off the original logical name, ...) lives on disk under a numeric ID.  The
# table-level mapping file `column_ids.json` is required to resolve those
# IDs back to logical names.  Before the fix, `column_ids.json` was not
# included in BACKUP / FREEZE, so RESTORE rebuilt a table that silently
# returned defaults for the affected column.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"
backup_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_b"

$CLIENT --query "DROP TABLE IF EXISTS t_backup SYNC"
$CLIENT --query "
CREATE TABLE t_backup (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
"

# Push c's column_id off identity ('c' -> numeric '1') and rename so the
# logical name also diverges from the on-disk column ID.  INSERT...VALUES
# is piped via stdin to avoid the known native-TCP hang on --query.
echo "INSERT INTO t_backup VALUES (1, 'x', 1.5)" | $CLIENT
$CLIENT --query "ALTER TABLE t_backup DROP COLUMN c"
$CLIENT --query "ALTER TABLE t_backup ADD COLUMN c Float64"
echo "INSERT INTO t_backup VALUES (2, 'y', 9.9)" | $CLIENT
$CLIENT --query "ALTER TABLE t_backup RENAME COLUMN c TO price"

echo "== before backup =="
$CLIENT --query "SELECT a, b, price FROM t_backup ORDER BY a"

$CLIENT --query "BACKUP TABLE t_backup TO Disk('backups', '${backup_name}')" > /dev/null
$CLIENT --query "DROP TABLE t_backup SYNC"
$CLIENT --query "RESTORE TABLE t_backup FROM Disk('backups', '${backup_name}')" > /dev/null

echo "== after restore =="
$CLIENT --query "SELECT a, b, price FROM t_backup ORDER BY a"
$CLIENT --query "SELECT column, column_id != column AS is_non_identity FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_backup' AND active AND column = 'price' ORDER BY name LIMIT 1"

# FREEZE round-trip: confirm column_ids.json lands next to the frozen parts.
$CLIENT --query "ALTER TABLE t_backup FREEZE WITH NAME '${backup_name}_freeze'"
clickhouse_root=$($CLIENT --query "SELECT path FROM system.disks WHERE name = 'default'")
shadow_path="${clickhouse_root}shadow/${backup_name}_freeze"
if find "${shadow_path}" -name 'column_ids.json' -type f 2>/dev/null | grep -q '.'; then
    echo "freeze_mapping_present"
else
    echo "freeze_mapping_missing"
fi

$CLIENT --query "ALTER TABLE t_backup UNFREEZE WITH NAME '${backup_name}_freeze'" > /dev/null
$CLIENT --query "DROP TABLE t_backup SYNC"

#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# why: column_ids.json must travel with BACKUP/FREEZE, RESTORE must not clobber or
# bypass a destination's active mapping, and cross-table part transfer must respect
# the ID counter -- otherwise non-identity columns silently read defaults or orphans.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"
backup_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_b"

# Scenario 1: BACKUP -> DROP -> RESTORE round-trips a non-identity column, and
# FREEZE places column_ids.json inside the shadow tree.
$CLIENT --query "DROP TABLE IF EXISTS t_backup SYNC"
$CLIENT --query "
CREATE TABLE t_backup (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
"

# Push c's column_id off identity ('c' -> numeric '1') and rename so the logical
# name also diverges from the on-disk column ID.
echo "INSERT INTO t_backup VALUES (1, 'x', 1.5)" | $CLIENT
$CLIENT --query "ALTER TABLE t_backup DROP COLUMN c"
$CLIENT --query "ALTER TABLE t_backup ADD COLUMN c Float64"
echo "INSERT INTO t_backup VALUES (2, 'y', 9.9)" | $CLIENT
$CLIENT --query "ALTER TABLE t_backup RENAME COLUMN c TO price"

$CLIENT --query "SELECT a, b, price FROM t_backup ORDER BY a"

$CLIENT --query "BACKUP TABLE t_backup TO Disk('backups', '${backup_name}')" > /dev/null
$CLIENT --query "DROP TABLE t_backup SYNC"
$CLIENT --query "RESTORE TABLE t_backup FROM Disk('backups', '${backup_name}')" > /dev/null

$CLIENT --query "SELECT a, b, price FROM t_backup ORDER BY a"
$CLIENT --query "SELECT column, column_id != column AS is_non_identity FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_backup' AND active AND column = 'price' ORDER BY name LIMIT 1"

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

# Scenario 2: RESTORE ... allow_non_empty_tables = 1 into a destination whose
# active mapping diverged from the backup's must throw, not silently rewrite.
backup2="${CLICKHOUSE_TEST_UNIQUE_NAME}_b2"
$CLIENT --query "DROP TABLE IF EXISTS t_restore SYNC"
$CLIENT --query "
CREATE TABLE t_restore (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
"

echo "INSERT INTO t_restore VALUES (1, 'x', 1.5)" | $CLIENT
$CLIENT --query "ALTER TABLE t_restore DROP COLUMN c"
$CLIENT --query "ALTER TABLE t_restore ADD COLUMN c Float64"
echo "INSERT INTO t_restore VALUES (2, 'y', 9.9)" | $CLIENT

$CLIENT --query "BACKUP TABLE t_restore TO Disk('backups', '${backup2}')" > /dev/null

# Diverge: another drop+add moves 'c' to a different column ID.
$CLIENT --query "ALTER TABLE t_restore DROP COLUMN c"
$CLIENT --query "ALTER TABLE t_restore ADD COLUMN c Float64"
echo "INSERT INTO t_restore VALUES (3, 'z', 3.3)" | $CLIENT

$CLIENT --query "
RESTORE TABLE t_restore FROM Disk('backups', '${backup2}')
SETTINGS allow_non_empty_tables = 1
" 2>&1 | grep -qE "differs from destination's active column-ID mapping" && echo "throws_on_mismatch" || echo "missing_guard"

# Existing destination data must still be readable with its own mapping.
$CLIENT --query "SELECT a, b, c FROM t_restore ORDER BY a"

$CLIENT --query "DROP TABLE t_restore SYNC"

# Scenario 3: same logical_to_id but the backup's counter is ahead (its history
# added then dropped a column, leaving orphan files).  RESTORE must bump the
# destination's counter so a later ADD COLUMN cannot reuse the orphan's ID.
backup3="${CLICKHOUSE_TEST_UNIQUE_NAME}_b3"
$CLIENT --query "DROP TABLE IF EXISTS t_orphan SYNC"
$CLIENT --query "
CREATE TABLE t_orphan (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
"
$CLIENT --query "ALTER TABLE t_orphan ADD COLUMN d UInt32 DEFAULT 0"
echo "INSERT INTO t_orphan (a, b, c, d) VALUES (1, 'x', 1.5, 42)" | $CLIENT
$CLIENT --query "ALTER TABLE t_orphan DROP COLUMN d"

$CLIENT --query "BACKUP TABLE t_orphan TO Disk('backups', '${backup3}')" > /dev/null
$CLIENT --query "DROP TABLE t_orphan SYNC"

$CLIENT --query "
CREATE TABLE t_orphan (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
"
echo "INSERT INTO t_orphan VALUES (10, 'y', 99)" | $CLIENT
# Fresh destination: same logical_to_id as the backup, but counter at 1.

$CLIENT --query "
RESTORE TABLE t_orphan FROM Disk('backups', '${backup3}')
SETTINGS allow_non_empty_tables = 1
" > /dev/null

# Without the counter bump, 'e' would get the orphan's ID and read 42 instead of 99.
$CLIENT --query "ALTER TABLE t_orphan ADD COLUMN e UInt32 DEFAULT 99"
$CLIENT --query "SELECT a, b, c, e FROM t_orphan ORDER BY a"

$CLIENT --query "DROP TABLE t_orphan SYNC"

# Scenario 4: a legacy backup with no column_ids.json restored into a destination
# with an active mapping must fail loudly, not attach logical-filename parts.
backup4="${CLICKHOUSE_TEST_UNIQUE_NAME}_b4"
$CLIENT --query "DROP TABLE IF EXISTS t_legacy SYNC"
$CLIENT --query "
CREATE TABLE t_legacy (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"
echo "INSERT INTO t_legacy VALUES (1, 'x', 1.5)" | $CLIENT
$CLIENT --query "BACKUP TABLE t_legacy TO Disk('backups', '${backup4}')" > /dev/null
$CLIENT --query "DROP TABLE t_legacy SYNC"

# Simulate a backup taken before the column-IDs feature by removing
# column_ids.json from the backup directory (current backups always include it).
backups_root=$($CLIENT --query "SELECT path FROM system.disks WHERE name = 'backups'")
find "${backups_root}${backup4}" -name 'column_ids.json' -type f -delete 2>/dev/null || true

$CLIENT --query "
CREATE TABLE t_legacy (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"
$CLIENT --query "ALTER TABLE t_legacy DROP COLUMN c"
$CLIENT --query "ALTER TABLE t_legacy ADD COLUMN c Float64"
echo "INSERT INTO t_legacy VALUES (10, 'y', 99)" | $CLIENT

$CLIENT --query "
RESTORE TABLE t_legacy FROM Disk('backups', '${backup4}')
SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1
" 2>&1 | grep -qE "backup has no .+ destination has an active" && echo "throws_on_legacy_into_active" || echo "missing_guard"

$CLIENT --query "DROP TABLE t_legacy SYNC"

# Scenario 5: ATTACH PARTITION FROM must reject a source whose next_column_id
# counter is ahead of the destination's (orphan-ID reuse hazard).
$CLIENT --query "DROP TABLE IF EXISTS t_attach_src SYNC"
$CLIENT --query "DROP TABLE IF EXISTS t_attach_dst SYNC"

$CLIENT --query "
CREATE TABLE t_attach_src (a UInt32, b String, c Float64)
ENGINE = MergeTree PARTITION BY tuple() ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"
# Push src counter ahead by adding then dropping a temporary column.
$CLIENT --query "ALTER TABLE t_attach_src ADD COLUMN d UInt32 DEFAULT 0"
echo "INSERT INTO t_attach_src (a, b, c, d) VALUES (1, 'x', 1.5, 42)" | $CLIENT
$CLIENT --query "ALTER TABLE t_attach_src DROP COLUMN d"

$CLIENT --query "
CREATE TABLE t_attach_dst (a UInt32, b String, c Float64)
ENGINE = MergeTree PARTITION BY tuple() ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"
echo "INSERT INTO t_attach_dst VALUES (10, 'y', 99)" | $CLIENT
# Both tables now have logical_to_id = {a:a,b:b,c:c}, but src's counter is past
# the orphan d's ID while dst's counter is still at 1.

$CLIENT --query "
ALTER TABLE t_attach_dst ATTACH PARTITION tuple() FROM t_attach_src
" 2>&1 | grep -qE "column-ID counter \([0-9]+\) is ahead" && echo "throws_on_counter_mismatch" || echo "missing_guard"

# Destination still readable with its own mapping.
$CLIENT --query "SELECT a, b, c FROM t_attach_dst ORDER BY a"

$CLIENT --query "DROP TABLE t_attach_src SYNC"
$CLIENT --query "DROP TABLE t_attach_dst SYNC"

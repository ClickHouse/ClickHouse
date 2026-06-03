#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas
# Scenario: RESTORE ... SETTINGS allow_non_empty_tables = 1 must not silently
# overwrite the destination's active column-ID mapping when it differs from
# the backup's mapping.  If it did, existing parts would be read with the
# wrong physical column names.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"
backup_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_b"

$CLIENT --query "DROP TABLE IF EXISTS t_restore SYNC"
$CLIENT --query "
CREATE TABLE t_restore (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
"

# Backup state: drop+add 'c' so 'c' has a non-identity column ID.
echo "INSERT INTO t_restore VALUES (1, 'x', 1.5)" | $CLIENT
$CLIENT --query "ALTER TABLE t_restore DROP COLUMN c"
$CLIENT --query "ALTER TABLE t_restore ADD COLUMN c Float64"
echo "INSERT INTO t_restore VALUES (2, 'y', 9.9)" | $CLIENT

$CLIENT --query "BACKUP TABLE t_restore TO Disk('backups', '${backup_name}')" > /dev/null

# Diverge: after backup, do another drop+add so destination mapping no longer
# matches the backup's mapping ('c' moves to a different column ID).
$CLIENT --query "ALTER TABLE t_restore DROP COLUMN c"
$CLIENT --query "ALTER TABLE t_restore ADD COLUMN c Float64"
echo "INSERT INTO t_restore VALUES (3, 'z', 3.3)" | $CLIENT

# RESTORE into the non-empty diverged table must throw, not silently rewrite.
echo "== restore with diverged mapping =="
$CLIENT --query "
RESTORE TABLE t_restore FROM Disk('backups', '${backup_name}')
SETTINGS allow_non_empty_tables = 1
" 2>&1 | grep -qE "differs from destination's active column-ID mapping" && echo "throws_on_mismatch" || echo "missing_guard"

# Existing destination data must still be readable with its own mapping.
$CLIENT --query "SELECT a, b, c FROM t_restore ORDER BY a"

$CLIENT --query "DROP TABLE t_restore SYNC"

# Scenario 2: same `logical_to_id`, backup has higher `next_column_id` because
# its history added then dropped a temporary column, leaving orphan files at a
# column ID the destination's counter has not yet handed out.  RESTORE must
# bump the counter so a later ADD COLUMN can't reuse that ID and read the
# orphan bytes.
backup2="${CLICKHOUSE_TEST_UNIQUE_NAME}_b2"
$CLIENT --query "DROP TABLE IF EXISTS t_orphan SYNC"
$CLIENT --query "
CREATE TABLE t_orphan (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
"
# Insert with a transient column 'd' so the part on disk carries an orphan
# file for d's column ID after the column is dropped.
$CLIENT --query "ALTER TABLE t_orphan ADD COLUMN d UInt32 DEFAULT 0"
echo "INSERT INTO t_orphan (a, b, c, d) VALUES (1, 'x', 1.5, 42)" | $CLIENT
$CLIENT --query "ALTER TABLE t_orphan DROP COLUMN d"
# Backup state: logical_to_id = {a:a,b:b,c:c}, counter past d's orphan ID.

$CLIENT --query "BACKUP TABLE t_orphan TO Disk('backups', '${backup2}')" > /dev/null
$CLIENT --query "DROP TABLE t_orphan SYNC"

$CLIENT --query "
CREATE TABLE t_orphan (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
"
echo "INSERT INTO t_orphan VALUES (10, 'y', 99)" | $CLIENT
# Fresh destination: same logical_to_id as backup, but counter at 1.

$CLIENT --query "
RESTORE TABLE t_orphan FROM Disk('backups', '${backup2}')
SETTINGS allow_non_empty_tables = 1
" > /dev/null

echo "== counter-bump scenario =="
# After RESTORE, ADD a new column.  Without the counter bump it would be
# allocated the same column ID as the orphan in the restored part, and
# reading it would return 42 instead of the default 99.
$CLIENT --query "ALTER TABLE t_orphan ADD COLUMN e UInt32 DEFAULT 99"
$CLIENT --query "SELECT a, b, c, e FROM t_orphan ORDER BY a"

$CLIENT --query "DROP TABLE t_orphan SYNC"

# Scenario 3: backup has no `column_ids.json` (legacy backup, e.g. taken before
# column IDs were enabled), restored into a destination with an active mapping.
# Without the guard, parts attach with legacy logical filenames and the reader
# silently returns defaults; the guard makes this fail loudly instead.
backup3="${CLICKHOUSE_TEST_UNIQUE_NAME}_b3"
$CLIENT --query "DROP TABLE IF EXISTS t_legacy SYNC"
$CLIENT --query "
CREATE TABLE t_legacy (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"
echo "INSERT INTO t_legacy VALUES (1, 'x', 1.5)" | $CLIENT
$CLIENT --query "BACKUP TABLE t_legacy TO Disk('backups', '${backup3}')" > /dev/null
$CLIENT --query "DROP TABLE t_legacy SYNC"

# Simulate a legacy backup taken before the column-IDs feature shipped by
# removing column_ids.json from the backup directory.  Backups produced by
# current code always include it, so the guard is otherwise unreachable.
backups_root=$($CLIENT --query "SELECT path FROM system.disks WHERE name = 'backups'")
find "${backups_root}${backup3}" -name 'column_ids.json' -type f -delete 2>/dev/null || true

# Destination: same name, but with column IDs active and pushed off identity.
$CLIENT --query "
CREATE TABLE t_legacy (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"
$CLIENT --query "ALTER TABLE t_legacy DROP COLUMN c"
$CLIENT --query "ALTER TABLE t_legacy ADD COLUMN c Float64"
echo "INSERT INTO t_legacy VALUES (10, 'y', 99)" | $CLIENT

echo "== restore legacy backup into active-mapping destination =="
$CLIENT --query "
RESTORE TABLE t_legacy FROM Disk('backups', '${backup3}')
SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1
" 2>&1 | grep -qE "backup has no .+ destination has an active" && echo "throws_on_legacy_into_active" || echo "missing_guard"

$CLIENT --query "DROP TABLE t_legacy SYNC"

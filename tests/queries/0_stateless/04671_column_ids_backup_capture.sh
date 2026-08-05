#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree
# no-parallel: `backup_pause_after_gathering_metadata` fires for every BACKUP on the server, so a
#   concurrent one could take this test's pause and make `SYSTEM WAIT FAILPOINT ... PAUSE` hang.
# why: under column IDs a RENAME COLUMN touches no part file, so nothing in the generic BACKUP
# machinery carries it. The archive is assembled from the schema captured while the table was locked;
# anything the table does after that capture belongs to a later instant.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"
FAILPOINT="backup_pause_after_gathering_metadata"
BACKUP_AS_OF="${CLICKHOUSE_TEST_UNIQUE_NAME}_as_of"
BACKUP_POST_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_post_id"
BACKUP_FOREIGN="${CLICKHOUSE_TEST_UNIQUE_NAME}_foreign"
BACKUP_REBIND="${CLICKHOUSE_TEST_UNIQUE_NAME}_rebind"

function cleanup()
{
    $CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" > /dev/null 2>&1 ||:
}
trap cleanup EXIT

# Leaves an ASYNC backup of $1 (id $2) stopped between the capture and the data phase, so the caller
# can land an operation in between. `ASYNC` returns immediately; the collector runs on a server
# thread, which is what pauses.
function start_paused_backup()
{
    $CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"
    $CLIENT --query "
        BACKUP TABLE $1 TO Disk('backups', '$2') SETTINGS id = '$2' ASYNC
    " > /dev/null
    $CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"
}

# Releases the pause and blocks until backup $1 leaves CREATING_BACKUP.
function finish_backup()
{
    $CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
    local deadline=$((SECONDS + 60))
    while [[ $SECONDS -lt $deadline ]]; do
        [[ "$($CLIENT --query "SELECT status FROM system.backups WHERE id = '$1' LIMIT 1")" != "CREATING_BACKUP" ]] && break
        sleep 0.1
    done
}

$CLIENT --query "
CREATE TABLE t_capture (a UInt32, b String)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"
# Push 'b' off identity so the rename below can only be followed through the mapping, not through
# the stream file names.
$CLIENT --query "ALTER TABLE t_capture DROP COLUMN b"
$CLIENT --query "ALTER TABLE t_capture ADD COLUMN b String"
echo "INSERT INTO t_capture VALUES (1, 'one'), (2, 'two')" | $CLIENT

# 1. A metadata-only RENAME landing after the capture must not reach the archive.
start_paused_backup t_capture "$BACKUP_AS_OF"
$CLIENT --query "ALTER TABLE t_capture RENAME COLUMN b TO renamed"
finish_backup "$BACKUP_AS_OF"
$CLIENT --query "SELECT status FROM system.backups WHERE id = '$BACKUP_AS_OF' LIMIT 1"

$CLIENT --query "RESTORE TABLE t_capture AS t_restored FROM Disk('backups', '$BACKUP_AS_OF')" > /dev/null
# The pre-rename name, and its rows under the ID they were written with. A post-capture read of the
# schema or the mapping would give `renamed` here instead.
$CLIENT --query "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_restored' ORDER BY name"
$CLIENT --query "SELECT a, b FROM t_restored ORDER BY a"

# 2. A mutation rewrites a part in place and keeps its block range, so a column added after the
# capture lands inside the block bound stamped with an ID the captured mapping cannot name. The
# backup must fail rather than archive something that cannot be restored.
start_paused_backup t_capture "$BACKUP_POST_ID"
$CLIENT --query "ALTER TABLE t_capture ADD COLUMN c Float64"
$CLIENT --query "ALTER TABLE t_capture UPDATE c = 3.3 WHERE 1 SETTINGS mutations_sync = 1"
finish_backup "$BACKUP_POST_ID"
$CLIENT --query "
SELECT status, error LIKE '%allocated after this backup captured%'
FROM system.backups WHERE id = '$BACKUP_POST_ID' LIMIT 1
"

# 3. A RESTORE landing after the capture installs a foreign mapping, rebinding the table's IDs so
# '1.bin' starts holding a different column -- with every ID still below the captured counter, which
# is why the per-part check above cannot see it. Only the block bound keeps those parts out.
$CLIENT --query "
CREATE TABLE t_foreign (k UInt32, a UInt32, b UInt32)
ENGINE = MergeTree ORDER BY k
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"
# Mirror image of t_rebind's assignment below: here 'b' takes ID 1 and 'a' takes ID 2.
$CLIENT --query "ALTER TABLE t_foreign DROP COLUMN b"
$CLIENT --query "ALTER TABLE t_foreign ADD COLUMN b UInt32"
$CLIENT --query "ALTER TABLE t_foreign DROP COLUMN a"
$CLIENT --query "ALTER TABLE t_foreign ADD COLUMN a UInt32"
echo "INSERT INTO t_foreign (k, a, b) VALUES (1, 77, 88)" | $CLIENT
$CLIENT --query "BACKUP TABLE t_foreign TO Disk('backups', '$BACKUP_FOREIGN')" > /dev/null

$CLIENT --query "
CREATE TABLE t_rebind (k UInt32, a UInt32, b UInt32)
ENGINE = MergeTree ORDER BY k
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"
$CLIENT --query "ALTER TABLE t_rebind DROP COLUMN a"
$CLIENT --query "ALTER TABLE t_rebind ADD COLUMN a UInt32"
$CLIENT --query "ALTER TABLE t_rebind DROP COLUMN b"
$CLIENT --query "ALTER TABLE t_rebind ADD COLUMN b UInt32"

# Empty at the capture, so the bound is 0 and no part arriving later can be archived.
start_paused_backup t_rebind "$BACKUP_REBIND"
$CLIENT --query "
RESTORE TABLE t_foreign AS t_rebind FROM Disk('backups', '$BACKUP_FOREIGN')
SETTINGS allow_different_table_def = 1
" > /dev/null
finish_backup "$BACKUP_REBIND"
$CLIENT --query "SELECT status FROM system.backups WHERE id = '$BACKUP_REBIND' LIMIT 1"
# 1: the rebinding restore did land inside the pause, which is what the assertion below rests on.
$CLIENT --query "SELECT count() FROM t_rebind"

$CLIENT --query "RESTORE TABLE t_rebind AS t_rebind_restored FROM Disk('backups', '$BACKUP_REBIND')" > /dev/null
# 0 rows: the restored parts belong to an instant after the capture, and reading them through the
# archived mapping would hand 'a' the bytes of 'b'.
$CLIENT --query "SELECT count() FROM t_rebind_restored"

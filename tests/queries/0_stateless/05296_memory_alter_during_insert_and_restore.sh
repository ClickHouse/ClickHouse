#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: arms the server-wide fail points restore_pause_before_data_restore_tasks and
# backup_pause_before_collecting_table_data.

# An INSERT that started before an ALTER renaming or dropping columns of a Memory table, and a RESTORE
# whose data is loaded after such an ALTER, store their rows under the column names after the ALTER.
# A BACKUP that took the table definition before such an ALTER fails.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

fifo="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.fifo"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT restore_pause_before_data_restore_tasks" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT backup_pause_before_collecting_table_data" 2>/dev/null
    rm -f "$fifo"
}
trap cleanup EXIT

# Streams the rows 1..50000 into `INSERT INTO $1`, runs the ALTER $2, then sends the rows 50001..50010.
# The first batch is larger than two pipe buffers and the client reads at most one before it receives the
# table structure, so writing it returns only after the server created the sink of the INSERT.
function insert_across_alter()
{
    rm -f "$fifo"
    mkfifo "$fifo"
    $CLICKHOUSE_CLIENT -q "INSERT INTO $1 SETTINGS async_insert = 0 FORMAT TSV" < "$fifo" &
    local client_pid=$!
    exec {fd}>"$fifo"
    seq 1 50000 >&${fd}
    $CLICKHOUSE_CLIENT -q "$2"
    seq 50001 50010 >&${fd}
    exec {fd}>&-
    wait $client_pid
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_rename (c0 UInt64) ENGINE = Memory"
insert_across_alter t_rename "ALTER TABLE t_rename RENAME COLUMN c0 TO c1"
$CLICKHOUSE_CLIENT -q "SELECT 'insert across rename', count(), countIf(c1 = 0), sum(c1) FROM t_rename"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_drop (a UInt64) ENGINE = Memory"
insert_across_alter t_drop "ALTER TABLE t_drop ADD COLUMN b UInt64, DROP COLUMN a"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_drop ADD COLUMN a UInt64"
$CLICKHOUSE_CLIENT -q "SELECT 'insert across drop', count(), sum(b), sum(a) FROM t_drop"

backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE t_backup (c0 UInt64) ENGINE = Memory;
INSERT INTO t_backup SELECT number + 1 FROM numbers(10);
"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_backup TO $backup" > /dev/null

# The restore creates `t_restored` and then pauses before loading its data.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT restore_pause_before_data_restore_tasks"
restore_id=$($CLICKHOUSE_CLIENT -q "RESTORE TABLE t_backup AS t_restored FROM $backup ASYNC" | cut -f1)
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT restore_pause_before_data_restore_tasks PAUSE"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_restored RENAME COLUMN c0 TO c1"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT restore_pause_before_data_restore_tasks"

for _ in $(seq 1 600); do
    status=$($CLICKHOUSE_CLIENT -q "SELECT status FROM system.backups WHERE id = '$restore_id'")
    [ "$status" = "RESTORED" ] || [ "$status" = "RESTORE_FAILED" ] && break
    sleep 0.1
done
echo "restore $status"
$CLICKHOUSE_CLIENT -q "SELECT 'restore across rename', count(), sum(c1) FROM t_restored"

# The backup takes the definition of `t_backup_alter` and then pauses before collecting its data.
$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE t_backup_alter (c0 UInt64) ENGINE = Memory;
INSERT INTO t_backup_alter SELECT number + 1 FROM numbers(10);
"
backup_alter="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_alter')"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT backup_pause_before_collecting_table_data"
backup_id=$($CLICKHOUSE_CLIENT -q "BACKUP TABLE t_backup_alter TO $backup_alter ASYNC" | cut -f1)
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT backup_pause_before_collecting_table_data PAUSE"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_backup_alter RENAME COLUMN c0 TO c1"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT backup_pause_before_collecting_table_data"

for _ in $(seq 1 600); do
    status=$($CLICKHOUSE_CLIENT -q "SELECT status FROM system.backups WHERE id = '$backup_id'")
    [ "$status" = "BACKUP_CREATED" ] || [ "$status" = "BACKUP_FAILED" ] && break
    sleep 0.1
done
echo "backup across rename $status"
$CLICKHOUSE_CLIENT -q "SELECT 'backup across rename error', error LIKE '%INCONSISTENT_METADATA_FOR_BACKUP%' FROM system.backups WHERE id = '$backup_id'"

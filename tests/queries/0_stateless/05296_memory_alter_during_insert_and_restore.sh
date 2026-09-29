#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: arms the server-wide fail points restore_pause_before_data_restore_tasks and
# backup_pause_before_collecting_table_data.

# An INSERT that started before an ALTER renaming or dropping columns of a Memory table, and a RESTORE
# whose data is loaded after such an ALTER, store their rows under the column names after the ALTER.
# A BACKUP that took the table definition before an ALTER changed its columns fails, and a materialized view
# whose inner Memory table was altered on its own is backed up and restored, unless a column of the view was renamed.

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

# Runs `BACKUP TABLE $1`, which takes the table definition and then pauses before collecting the data, runs `$2` in
# the pause, and prints the status of the backup and whether it failed with INCONSISTENT_METADATA_FOR_BACKUP.
function backup_across_alter()
{
    local backup_id status
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT backup_pause_before_collecting_table_data"
    backup_id=$($CLICKHOUSE_CLIENT -q "BACKUP TABLE $1 TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_$1') ASYNC" | cut -f1)
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT backup_pause_before_collecting_table_data PAUSE"
    $CLICKHOUSE_CLIENT -q "$2"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT backup_pause_before_collecting_table_data"
    for _ in $(seq 1 600); do
        status=$($CLICKHOUSE_CLIENT -q "SELECT status FROM system.backups WHERE id = '$backup_id'")
        [ "$status" = "BACKUP_CREATED" ] || [ "$status" = "BACKUP_FAILED" ] && break
        sleep 0.1
    done
    echo "$3 $status"
    $CLICKHOUSE_CLIENT -q "SELECT '$3 error', error LIKE '%INCONSISTENT_METADATA_FOR_BACKUP%' FROM system.backups WHERE id = '$backup_id'"
}

$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE t_backup_alter (c0 UInt64) ENGINE = Memory;
INSERT INTO t_backup_alter SELECT number + 1 FROM numbers(10);
CREATE TABLE t_backup_modify (c0 UInt64) ENGINE = Memory;
INSERT INTO t_backup_modify SELECT number + 1 FROM numbers(10);
"
backup_across_alter t_backup_alter "ALTER TABLE t_backup_alter RENAME COLUMN c0 TO c1" "backup across rename"
backup_across_alter t_backup_modify "ALTER TABLE t_backup_modify MODIFY COLUMN c0 Nullable(UInt64)" "backup across type change"

# A materialized view whose inner Memory table was altered on its own is backed up and restored under the view's columns.
$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE t_mv_src (x UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW t_mv ENGINE = Memory AS SELECT x FROM t_mv_src;
INSERT INTO t_mv_src SELECT number + 1 FROM numbers(3);
"
inner=$($CLICKHOUSE_CLIENT -q "SELECT target_table FROM system.tables WHERE database = currentDatabase() AND name = 't_mv'")
$CLICKHOUSE_CLIENT -q "ALTER TABLE \`$inner\` ADD COLUMN y UInt64"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mv_src VALUES (4)"
backup_mv="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_mv')"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_mv TO $backup_mv" > /dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mv SYNC"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE t_mv FROM $backup_mv" > /dev/null
$CLICKHOUSE_CLIENT -q "SELECT 'materialized view with an altered inner table', count(), sum(x) FROM t_mv"

# The RESTORE of a view whose column was renamed in its inner table fails instead of restoring the column as default values,
# and a column dropped from the inner table is restored as default values, not with its old data.
$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE t_mv_src2 (x UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW t_mv_rename ENGINE = Memory AS SELECT x FROM t_mv_src2;
CREATE MATERIALIZED VIEW t_mv_drop ENGINE = Memory AS SELECT x, x * 10 AS z FROM t_mv_src2;
INSERT INTO t_mv_src2 SELECT number + 1 FROM numbers(3);
"
inner_rename=$($CLICKHOUSE_CLIENT -q "SELECT target_table FROM system.tables WHERE database = currentDatabase() AND name = 't_mv_rename'")
inner_drop=$($CLICKHOUSE_CLIENT -q "SELECT target_table FROM system.tables WHERE database = currentDatabase() AND name = 't_mv_drop'")
$CLICKHOUSE_CLIENT -q "ALTER TABLE \`$inner_rename\` RENAME COLUMN x TO y"
$CLICKHOUSE_CLIENT -q "ALTER TABLE \`$inner_drop\` DROP COLUMN z"
backup_mv2="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_mv2')"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_mv_rename, TABLE t_mv_drop TO $backup_mv2" > /dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mv_rename SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mv_drop SYNC"
echo "view with a renamed inner column $($CLICKHOUSE_CLIENT -q "RESTORE TABLE t_mv_rename FROM $backup_mv2" 2>&1 | grep -o -m1 CANNOT_RESTORE_TABLE)"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE t_mv_drop FROM $backup_mv2" > /dev/null
$CLICKHOUSE_CLIENT -q "SELECT 'view with a dropped inner column', count(), sum(x), sum(z) FROM t_mv_drop"

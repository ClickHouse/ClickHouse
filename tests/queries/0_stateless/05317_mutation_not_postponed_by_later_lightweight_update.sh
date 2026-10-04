#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# no-parallel, no-fasttest: uses a server-wide failpoint that pauses the next lightweight update, whichever table it is on.
# no-replicated-database, no-shared-merge-tree: the test is about the mutation selection of the plain `MergeTree`.

# A lightweight `UPDATE` that started after an `ALTER ... UPDATE` must not hold that mutation back
# while it is still running; both changes must be visible at the end.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FAILPOINT="mt_lightweight_update_pause_after_block_allocation"
TABLE="t_mutation_later_lwu"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $TABLE SYNC;

    CREATE TABLE $TABLE (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    INSERT INTO $TABLE SELECT number, 1, 0 FROM numbers(1000);

    SYSTEM STOP MERGES $TABLE;
    ALTER TABLE $TABLE UPDATE w = 5 WHERE 1 SETTINGS mutations_sync = 0;
    SYSTEM ENABLE FAILPOINT $FAILPOINT;
"

# The update pauses after it reserved a block number above the version of the mutation.
$CLICKHOUSE_CLIENT --enable_lightweight_update 1 --query "UPDATE $TABLE SET v = 2 WHERE v = 1" &
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES $TABLE"

for _ in $(seq 1 300)
do
    unmutated=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = '$TABLE' AND active AND NOT startsWith(name, 'patch') AND data_version = min_block_number")
    [[ "$unmutated" == "0" ]] && break
    sleep 0.2
done

$CLICKHOUSE_CLIENT --query "
    SELECT 'while the update is paused: unmutated parts', count() FROM system.parts
    WHERE database = currentDatabase() AND table = '$TABLE' AND active AND NOT startsWith(name, 'patch') AND data_version = min_block_number;
    SELECT 'while the update is paused: patch parts', count() FROM system.parts
    WHERE database = currentDatabase() AND table = '$TABLE' AND active AND startsWith(name, 'patch');
    SELECT 'while the update is paused: w = 5', count() FROM $TABLE WHERE w = 5 SETTINGS apply_mutations_on_fly = 0;
"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
wait

$CLICKHOUSE_CLIENT --query "
    SELECT 'the lightweight update applied', count() FROM $TABLE WHERE v = 2;
    SELECT 'the mutation applied', count() FROM $TABLE WHERE w = 5;
    DROP TABLE $TABLE SYNC;
"

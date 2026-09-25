#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: uses a server-wide failpoint that pauses the next lightweight update, whichever table it is on.
# no-replicated-database, no-shared-merge-tree: the test is about the mutation selection of the plain `MergeTree`.

# A heavyweight `ALTER ... UPDATE` that ran while a lightweight `UPDATE` was still writing its patch
# part silently dropped the acknowledged lightweight update on a plain `MergeTree`: the mutation read
# the part without the patch and wrote a part at a higher data version, which the patch no longer
# applied to. `ReplicatedMergeTree` postpones such a mutation while a lightweight update with a lower
# block number is uncommitted; the plain engine selected the part regardless.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FAILPOINT="mt_lightweight_update_pause_after_block_allocation"
TABLE="t_mutation_waits_lwu"

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
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;

    INSERT INTO $TABLE SELECT number, 1, 0 FROM numbers(20000);

    SYSTEM ENABLE FAILPOINT $FAILPOINT;
"

# The update reserves its block number and pauses right there, before its patch part is written.
$CLICKHOUSE_CLIENT --enable_lightweight_update 1 --query "UPDATE $TABLE SET v = 2 WHERE v = 1" &

# The handshake: the failpoint is reached only after `allocateBlockNumber(CommittingBlock::Op::Update)`,
# so from here on the update is known to hold a block number below the version of the mutation.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE UPDATE w = 5 WHERE 1 SETTINGS mutations_sync = 0"

# The mutation must be left alone until the update is committed.
for _ in $(seq 1 300)
do
    postponed=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = '$TABLE' AND NOT is_done
            AND arrayExists(x -> x LIKE 'Lightweight update%', mapValues(parts_postpone_reasons))")
    [[ "$postponed" == "1" ]] && break
    sleep 0.2
done

$CLICKHOUSE_CLIENT --query "
    SELECT 'the mutation is postponed:', arrayDistinct(mapValues(parts_postpone_reasons)) FROM system.mutations
    WHERE database = currentDatabase() AND table = '$TABLE';
"

# Let the update commit its patch part; the mutation is woken up by the release of the block number.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
wait

for _ in $(seq 1 300)
do
    pending=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE' AND NOT is_done")
    [[ "$pending" == "0" ]] && break
    sleep 0.2
done

$CLICKHOUSE_CLIENT --query "
    SELECT 'the lightweight update survived', count() FROM $TABLE WHERE v = 2;
    SELECT 'and the mutation applied', count() FROM $TABLE WHERE w = 5;
    SELECT 'rows', count() FROM $TABLE;
    SELECT 'pending mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE' AND NOT is_done;
    DROP TABLE $TABLE SYNC;
"

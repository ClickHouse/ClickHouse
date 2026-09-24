#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: uses a server-wide failpoint that pauses every lightweight update.
# no-replicated-database, no-shared-merge-tree: the test is about the mutation selection of the plain `MergeTree`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A lightweight update reserves its block number before its patch part is committed. A mutation
# with a higher version must wait for the update: otherwise it is executed over the source part
# without the patch, the patch is never applied to the result (which has a higher data version),
# and a mutation with a lower version squashed into the same task would see the patch too early.

FAILPOINT="mt_lightweight_update_pause_after_block_allocation"
TABLE="t_mutation_over_pending_lwu"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $TABLE SYNC;

    CREATE TABLE $TABLE (id UInt64, c UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    INSERT INTO $TABLE VALUES (1, 1), (5, 2);

    SYSTEM STOP MERGES $TABLE;
    ALTER TABLE $TABLE UPDATE c = c + 100 WHERE 1 SETTINGS mutations_sync = 0;

    SYSTEM ENABLE FAILPOINT $FAILPOINT;
"

# The update reserves its block number (which is above the version of the mutation above) and pauses.
$CLICKHOUSE_CLIENT --query "UPDATE $TABLE SET c = 0 WHERE 1" &

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

# Queued while the update is still in flight, so it gets a version above the update.
$CLICKHOUSE_CLIENT --query "
    ALTER TABLE $TABLE DELETE WHERE 0 SETTINGS mutations_sync = 0;
    SYSTEM START MERGES $TABLE;
"

# The first mutation is executed, the second one is postponed until the update is committed.
for _ in $(seq 1 300)
do
    postponed=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = '$TABLE' AND NOT is_done
            AND arrayExists(x -> x LIKE 'Lightweight update%', mapValues(parts_postpone_reasons))")
    [[ "$postponed" == "1" ]] && break
    sleep 0.2
done

# A part inserted after the update reserved its block number has a data version above the update, while the
# first mutation (below the update) is still tracked. A new mutation for that part must be postponed as well.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO $TABLE VALUES (3, 3);
    ALTER TABLE $TABLE DELETE WHERE 0 SETTINGS mutations_sync = 0;
"

for _ in $(seq 1 300)
do
    postponed=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = '$TABLE' AND NOT is_done
            AND arrayExists(x -> x LIKE 'Lightweight update%', mapValues(parts_postpone_reasons))")
    [[ "$postponed" == "2" ]] && break
    sleep 0.2
done

$CLICKHOUSE_CLIENT --query "
    SELECT mutation_id, is_done, arrayDistinct(mapValues(parts_postpone_reasons)) FROM system.mutations
    WHERE database = currentDatabase() AND table = '$TABLE' ORDER BY mutation_id;
"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
wait

$CLICKHOUSE_CLIENT --query "
    ALTER TABLE $TABLE DELETE WHERE 0 SETTINGS mutations_sync = 2;

    SELECT 'on the fly';
    SELECT id, c FROM $TABLE ORDER BY id SETTINGS apply_mutations_on_fly = 1;

    SELECT 'materialized';
    SELECT id, c FROM $TABLE ORDER BY id SETTINGS apply_patch_parts = 0;

    SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE' AND NOT is_done;

    DROP TABLE $TABLE SYNC;
"

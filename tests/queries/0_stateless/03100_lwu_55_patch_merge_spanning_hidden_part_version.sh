#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
# zookeeper: needs ReplicatedMergeTree
# no-parallel: holds the server wide PAUSEABLE failpoint rmt_mutate_task_pause_in_prepare, which would also
# pause a concurrent test's replicated mutation
# no-shared-merge-tree: relies on the merge assignment of ReplicatedMergeTree
# no-replicated-database: fails due to additional shard

# The data versions that a merge of patch parts must not span were read from `virtual_parts`, which keeps
# only the topmost covering part of a block range. A version was therefore invisible while an entry covering
# the part sat in the replication queue, the merge was allowed, and every later command on the partition
# failed with "Found patch part ... that intersects mutation with version ...".
# Related: https://github.com/ClickHouse/ClickHouse/issues/98898

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

# The failpoint is server wide, so release it however this test ends.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_in_prepare"' EXIT

# The patch partition id is hashed from the structure of the patch, hence read it instead of spelling it out.
function patch_partition_of()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT DISTINCT partition_id FROM system.parts
        WHERE database = currentDatabase() AND table = '$1' AND active AND startsWith(partition_id, 'patch')"
}

function wait_for_unfinished_mutation()
{
    local table=$1
    for _ in {1..300}
    do
        local mutation_id
        mutation_id=$($CLICKHOUSE_CLIENT --query "
            SELECT mutation_id FROM system.mutations
            WHERE database = currentDatabase() AND table = '$table' AND NOT is_done
            ORDER BY mutation_id DESC LIMIT 1")

        if [[ -n "$mutation_id" ]]; then
            echo "$mutation_id"
            return
        fi
        sleep 0.3
    done

    echo "Timed out while waiting for a mutation of $table to appear" >&2
}

function wait_for_mutations_gone()
{
    local table=$1
    for _ in {1..300}
    do
        if [[ "$($CLICKHOUSE_CLIENT --query "
                SELECT count() FROM system.mutations
                WHERE database = currentDatabase() AND table = '$table'")" == "0" ]]; then
            return
        fi
        sleep 0.3
    done

    echo "Timed out while waiting for the mutations of $table to be forgotten"
    $CLICKHOUSE_CLIENT --query "
        SELECT * FROM system.mutations
        WHERE database = currentDatabase() AND table = '$table' FORMAT Vertical"
}

function wait_for_queue_type_drained()
{
    local table=$1
    local entry_type=$2
    for _ in {1..300}
    do
        if [[ "$($CLICKHOUSE_CLIENT --query "
                SELECT count() FROM system.replication_queue
                WHERE database = currentDatabase() AND table = '$table' AND type = '$entry_type'")" == "0" ]]; then
            return
        fi
        sleep 0.3
    done

    echo "Timed out while waiting for $entry_type entries of $table to drain"
    $CLICKHOUSE_CLIENT --query "
        SELECT * FROM system.replication_queue
        WHERE database = currentDatabase() AND table = '$table' FORMAT Vertical"
}

# OPTIMIZE retries the selection ten times and waits for the replication queue between attempts, so bound
# that wait: in every arm below the queue holds an entry that is deliberately not going to be executed.
optimize_settings="optimize_throw_if_noop = 0, receive_timeout = 5"

##############################################################################################
echo "-- arm 1: the data version a part still carries, hidden by a queued mutation"
##############################################################################################

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_lwu_hidden (id UInt64, c1 UInt64, c2 UInt64)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_hidden/', '1')
    ORDER BY id
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        -- Keep the patch that the mutation below applies, so that it is still there when the newer patches
        -- are merged.
        remove_unused_patch_parts = 0,
        -- Only the explicit OPTIMIZE below may merge the patch parts.
        max_bytes_to_merge_at_max_space_in_pool = 1;

    SET insert_keeper_fault_injection_probability = 0;
    INSERT INTO t_lwu_hidden SELECT number, number, number FROM numbers(10);

    SET enable_lightweight_update = 1;
    UPDATE t_lwu_hidden SET c1 = 100 WHERE id = 1;

    -- A regular mutation gives the part a data version above that patch and applies it.
    ALTER TABLE t_lwu_hidden UPDATE c2 = 200 WHERE id = 2 SETTINGS mutations_sync = 2;

    -- A finished mutation is eventually forgotten while the data version it gave to the parts stays;
    -- \`KILL MUTATION\` reaches that state at once. Without it the merge below is refused by the mutation
    -- check alone and the case this test guards against is not reached.
    KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_hidden' SYNC FORMAT Null;

    UPDATE t_lwu_hidden SET c1 = 300 WHERE id = 3;
    UPDATE t_lwu_hidden SET c1 = 400 WHERE id = 4;
"

# The paused entry puts a part that covers the one on disk into the queue, so the version the part still
# carries is the topmost one no longer.
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_mutate_task_pause_in_prepare;
    ALTER TABLE t_lwu_hidden UPDATE c2 = 500 WHERE id = 5;
"

hidden_mutation=$(wait_for_unfinished_mutation t_lwu_hidden)
wait_for_mutation_in_progress "t_lwu_hidden" "$hidden_mutation"

patch_partition=$(patch_partition_of t_lwu_hidden)
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_lwu_hidden PARTITION ID '$patch_partition' FINAL SETTINGS $optimize_settings"

# The patch that the mutation has already applied must not be merged with the newer ones, so more than one
# patch part is left. A merge of all of them spans the data version of the part.
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 1 FROM system.parts
    WHERE database = currentDatabase() AND table = 't_lwu_hidden' AND active AND startsWith(name, 'patch');

    SELECT id, c1 FROM t_lwu_hidden ORDER BY id;
"

# The patches newer than the part are legitimately not applied to it yet, so this is refused with the
# ordinary message that says how to proceed. Merging them across the hidden version made it the logical
# error instead, and then nothing could be done with the partition at all.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_lwu_hidden DETACH PARTITION ID 'all'" 2>&1 \
    | grep -o -m 1 -e 'unapplied patch parts' -e 'intersects mutation with version' ||:

##############################################################################################
echo "-- arm 2: the data version a queued mutation is going to assign"
##############################################################################################

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_lwu_future (id UInt64, c1 UInt64, c2 UInt64)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_future/', '1')
    ORDER BY id
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        remove_unused_patch_parts = 0,
        max_bytes_to_merge_at_max_space_in_pool = 1;

    SET insert_keeper_fault_injection_probability = 0;
    INSERT INTO t_lwu_future SELECT number, number, number FROM numbers(10);

    SET enable_lightweight_update = 1;
    UPDATE t_lwu_future SET c1 = 100 WHERE id = 1;

    -- This mutation is never executed while the patches below are merged, so the data version it assigns is
    -- named by its queue entry only.
    SYSTEM ENABLE FAILPOINT rmt_mutate_task_pause_in_prepare;
    ALTER TABLE t_lwu_future UPDATE c2 = 200 WHERE id = 2;
"

future_mutation=$(wait_for_unfinished_mutation t_lwu_future)
wait_for_mutation_in_progress "t_lwu_future" "$future_mutation"

$CLICKHOUSE_CLIENT --query "
    SET enable_lightweight_update = 1;
    UPDATE t_lwu_future SET c1 = 300 WHERE id = 3;
    UPDATE t_lwu_future SET c1 = 400 WHERE id = 4;

    -- Killing the mutation leaves its queue entry behind, which is the state the reported failure was in.
    KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_future' AND mutation_id = '$future_mutation' FORMAT Null;
"

# Only once the mutation is forgotten does the merge below depend on the collected versions alone.
wait_for_mutations_gone t_lwu_future

# The entry the killed mutation left behind is still there to be read.
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 't_lwu_future' AND type = 'MUTATE_PART'"

patch_partition=$(patch_partition_of t_lwu_future)
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_lwu_future PARTITION ID '$patch_partition' FINAL SETTINGS $optimize_settings"

$CLICKHOUSE_CLIENT --query "
    SELECT count() > 1 FROM system.parts
    WHERE database = currentDatabase() AND table = 't_lwu_future' AND active AND startsWith(name, 'patch')"

# Let the entry run and check the invariant itself, not just the refusal. The entry advances the data
# version of the part even though the mutation it belonged to was killed, so that version now lies strictly
# inside the range that a merge of the patch parts would have produced.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_in_prepare"
wait_for_queue_type_drained t_lwu_future MUTATE_PART

$CLICKHOUSE_CLIENT --query "
    SELECT
        maxIf(data_version, NOT startsWith(name, 'patch')) > minIf(data_version, startsWith(name, 'patch'))
        AND maxIf(data_version, NOT startsWith(name, 'patch')) < maxIf(data_version, startsWith(name, 'patch'))
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_lwu_future' AND active"

$CLICKHOUSE_CLIENT --query "ALTER TABLE t_lwu_future DETACH PARTITION ID 'all'" 2>&1 \
    | grep -o -m 1 -e 'unapplied patch parts' -e 'intersects mutation with version' ||:

$CLICKHOUSE_CLIENT --query "DROP TABLE t_lwu_future SYNC"

##############################################################################################
echo "-- arm 3: an intermediate data version in a chain of queued entries"
##############################################################################################

# A replica assigns at most one successor per part, so the chain has to arrive through the log. The second
# replica must still fetch the patch parts, which is why merges rather than the whole queue are stopped.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_chain_1 (id UInt64, c1 UInt64, c2 UInt64)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_chain/', '1')
    ORDER BY id
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        remove_unused_patch_parts = 0,
        max_bytes_to_merge_at_max_space_in_pool = 1;

    CREATE TABLE t_chain_2 (id UInt64, c1 UInt64, c2 UInt64)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_chain/', '2')
    ORDER BY id
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        remove_unused_patch_parts = 0,
        max_bytes_to_merge_at_max_space_in_pool = 1;

    SYSTEM STOP MERGES t_chain_2;

    SET insert_keeper_fault_injection_probability = 0;
    INSERT INTO t_chain_1 SELECT number, number, number FROM numbers(10);

    SET enable_lightweight_update = 1;
    UPDATE t_chain_1 SET c1 = 100 WHERE id = 1;

    -- Two mutations, both executed here and both only queued on the other replica. Its queue then names
    -- the result of each of them for the same block range, while only the later one covers.
    ALTER TABLE t_chain_1 UPDATE c2 = 200 WHERE id = 2 SETTINGS mutations_sync = 1;

    UPDATE t_chain_1 SET c1 = 300 WHERE id = 3;
    UPDATE t_chain_1 SET c1 = 400 WHERE id = 4;

    ALTER TABLE t_chain_1 UPDATE c2 = 500 WHERE id = 5 SETTINGS mutations_sync = 1;

    KILL MUTATION WHERE database = currentDatabase() AND table = 't_chain_1' FORMAT Null;
"

$CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA t_chain_2 LIGHTWEIGHT"
wait_for_mutations_gone t_chain_2

# Both mutation entries are queued on the lagging replica.
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 't_chain_2' AND type = 'MUTATE_PART'"

patch_partition=$(patch_partition_of t_chain_2)
$CLICKHOUSE_CLIENT --query "
    OPTIMIZE TABLE t_chain_2 PARTITION ID '$patch_partition' FINAL
    SETTINGS optimize_throw_if_noop = 1, alter_sync = 0, receive_timeout = 5" 2>&1 \
    | grep -o -m 1 'would span data version' ||:

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 't_chain_2'
      AND type = 'MERGE_PARTS' AND new_part_name LIKE 'patch%';

    SYSTEM START MERGES t_chain_2;
    DROP TABLE t_chain_1 SYNC;
    DROP TABLE t_chain_2 SYNC;
"

##############################################################################################
echo "-- arm 4: the same merge is allowed once no version is hidden"
##############################################################################################

wait_for_mutation "t_lwu_hidden" "$hidden_mutation"

patch_partition=$(patch_partition_of t_lwu_hidden)
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_lwu_hidden PARTITION ID '$patch_partition' FINAL SETTINGS optimize_throw_if_noop = 0"

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_lwu_hidden' AND active AND startsWith(name, 'patch');

    ALTER TABLE t_lwu_hidden DETACH PARTITION ID 'all';
    SELECT count() FROM t_lwu_hidden;

    DROP TABLE t_lwu_hidden SYNC;
"

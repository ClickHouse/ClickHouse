#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
# Tag no-parallel: Fails due to failpoint intersection
# no-replicated-database: Fails due to additional replicas or shards

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

# The replicated arm of 04649. rmt_merge_selecting_task_max_part_size forces the free-space budget of
# every mutation to 1 byte, so every part exceeds it. A mutation that only hardlinks the files it does
# not touch must be admitted anyway; anything that rewrites the part must still be postponed.

# The failpoints are server-wide, so leaving one enabled after an early exit under `set -e` would give
# every later mutation on this server a 1 byte budget, or leave a mutation paused forever. The queue
# stop below is table-scoped, but an early exit between stopping and starting it would leave the DROP
# of that table waiting forever.
trap '$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_max_part_size;
" 2>/dev/null || true
$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_in_prepare;
" 2>/dev/null || true
$CLICKHOUSE_CLIENT --query "
    SYSTEM START REPLICATION QUEUES rmt_fetch_2;
" 2>/dev/null || true' EXIT

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE rmt_drop_index (event String, id UInt64,
        INDEX idx_event event TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/rmt_drop_index/', '1') ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             packed_skip_index_max_bytes = 0,
             merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

    INSERT INTO rmt_drop_index SELECT repeat('abcdefgh', 20), number FROM numbers(20000);
    OPTIMIZE TABLE rmt_drop_index FINAL;
"

$CLICKHOUSE_CLIENT --query "
    SELECT 'fixture', part_type, part_storage_type FROM system.parts
    WHERE database = currentDatabase() AND table = 'rmt_drop_index' AND active;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_max_part_size;
    ALTER TABLE rmt_drop_index DROP INDEX idx_event SETTINGS alter_sync = 0;
"

wait_for_mutation "rmt_drop_index" "0000000000"

# Liveness plus the route oracle: the entry must not have completed by rewriting the whole part, which
# needs the space it was refused for.
$CLICKHOUSE_CLIENT --query "
    SELECT 'rmt_drop_index', 'pending', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'rmt_drop_index' AND NOT is_done;

    SELECT 'rmt_drop_index', 'failed', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'rmt_drop_index' AND notEmpty(latest_fail_reason);

    SELECT 'rmt_drop_index', 'indices_left', count() FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 'rmt_drop_index';

    SELECT 'rmt_drop_index', 'rows', count() FROM rmt_drop_index;

    SYSTEM FLUSH LOGS part_log;
    SELECT 'rmt_drop_index', 'route_partial', sum(ProfileEvents['MutationSomePartColumns']) > 0,
        'route_full', sum(ProfileEvents['MutationAllPartColumns'])
    FROM system.part_log
    WHERE database = currentDatabase() AND table = 'rmt_drop_index' AND event_type = 'MutatePart';
"
$CLICKHOUSE_CLIENT --query "CHECK TABLE rmt_drop_index" | while read -r line; do
    echo "rmt_drop_index	check	$line"
done

# Negative control: a DELETE rewrites the part, so it must still be refused for the same reason.
$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE rmt_delete (id UInt64, s String, INDEX idx_id id TYPE minmax GRANULARITY 1)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/rmt_delete/', '1') ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             packed_skip_index_max_bytes = 0,
             merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

    INSERT INTO rmt_delete SELECT number, repeat('x', 100) FROM numbers(20000);
    OPTIMIZE TABLE rmt_delete FINAL;
    ALTER TABLE rmt_delete DELETE WHERE id = 1 SETTINGS alter_sync = 0;
"

# The reported reason is re-read until it appears, rather than sampled once after a weaker wait: the
# replicated merge-selecting task clears the postpone reasons at the top of every iteration and refills
# them later under a separate acquisition of state_mutex, so a read landing in that window sees an empty
# map. Waiting for any reason and then reading the specific one in a second query leaves that window open
# between the two. A reason that never appears still fails, because only the last read is reported.
for _ in $(seq 1 300); do
    postponed=$($CLICKHOUSE_CLIENT --query "
        SELECT 'rmt_delete', 'postponed',
            arrayExists(reason -> reason = 'Exceed max source part size', mapValues(parts_postpone_reasons)),
            'no_failure', empty(latest_fail_reason)
        FROM system.mutations
        WHERE database = currentDatabase() AND table = 'rmt_delete' AND NOT is_done
    ")
    if [ "$(echo "$postponed" | cut -f3)" = "1" ]; then
        break
    fi
    sleep 0.1
done
echo "$postponed"

##########################################################################################
# Fetch suppression. An exempt mutation must run LOCALLY even when the result part is already
# available elsewhere: the fetch would reserve the sender's whole part, which on the full disk this
# feature exists for can never succeed - the reported bug with extra steps. Two replicas of one table
# are needed because findReplicaHavingPart skips the replica itself, so with a single replica the
# `!hardlink_only` conjunct in MutateFromLogEntryTask is unreachable and deleting it changes nothing.
#
# prefer_fetch_merged_part_time_threshold = 0 and prefer_fetch_merged_part_size_threshold = 0 force
# the fetch preference on whenever it CAN apply (defaults are 3600 s and 10 GiB), so the only thing
# left standing between the entry and a fetch is the exemption itself.
##########################################################################################

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE rmt_fetch_1 (event String, id UInt64,
        INDEX idx_event event TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/rmt_fetch/', '1') ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             packed_skip_index_max_bytes = 0,
             prefer_fetch_merged_part_time_threshold = 0, prefer_fetch_merged_part_size_threshold = 0,
             merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

    CREATE TABLE rmt_fetch_2 (event String, id UInt64,
        INDEX idx_event event TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/rmt_fetch/', '2') ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             packed_skip_index_max_bytes = 0,
             prefer_fetch_merged_part_time_threshold = 0, prefer_fetch_merged_part_size_threshold = 0,
             merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

    INSERT INTO rmt_fetch_1 SELECT repeat('abcdefgh', 20), number FROM numbers(20000);
    SYSTEM SYNC REPLICA rmt_fetch_2;
    OPTIMIZE TABLE rmt_fetch_1 FINAL;
    SYSTEM SYNC REPLICA rmt_fetch_2;
"

# Hold replica 2 back so replica 1 finishes the mutation first and the result part exists to be
# fetched. Without this ordering the two race and replica 2 usually mutates before anyone could have
# offered it a fetch, which would make the case pass for the wrong reason.
$CLICKHOUSE_CLIENT --query "SYSTEM STOP REPLICATION QUEUES rmt_fetch_2"

$CLICKHOUSE_CLIENT --query "
    ALTER TABLE rmt_fetch_1 DROP INDEX idx_event SETTINGS alter_sync = 1, mutations_sync = 1;
"

# Replica 1 is done, so its result part is now a fetch candidate for replica 2.
$CLICKHOUSE_CLIENT --query "
    SELECT 'rmt_fetch', 'source_ready', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'rmt_fetch_1' AND is_done;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_max_part_size;
    SYSTEM START REPLICATION QUEUES rmt_fetch_2;
"

wait_for_mutation "rmt_fetch_2" "0000000000"

# The route oracle plus the absence of any fetch. A fetch produces a DownloadPart entry and no
# partial-route MutatePart event at all, so the two together pin the entry to the local partial route.
$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS part_log;

    SELECT 'rmt_fetch', 'route_partial', sum(ProfileEvents['MutationSomePartColumns']) > 0,
        'route_full', sum(ProfileEvents['MutationAllPartColumns'])
    FROM system.part_log
    WHERE database = currentDatabase() AND table = 'rmt_fetch_2' AND event_type = 'MutatePart';

    /* Scoped to the mutation's RESULT part, which is the active one: replica 2 also downloads the
       pre-mutation part when it first syncs, and that fetch is expected. */
    SELECT 'rmt_fetch', 'downloads_of_result', count() FROM system.part_log
    WHERE database = currentDatabase() AND table = 'rmt_fetch_2' AND event_type = 'DownloadPart'
      AND part_name IN (SELECT name FROM system.parts
          WHERE database = currentDatabase() AND table = 'rmt_fetch_2' AND active);

    SELECT 'rmt_fetch', 'pending', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'rmt_fetch_2' AND NOT is_done;

    SELECT 'rmt_fetch', 'failed', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'rmt_fetch_2' AND notEmpty(latest_fail_reason);

    SELECT 'rmt_fetch', 'indices_left', count() FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 'rmt_fetch_2';

    SELECT 'rmt_fetch', 'rows', count() FROM rmt_fetch_2;
"
$CLICKHOUSE_CLIENT --query "CHECK TABLE rmt_fetch_2" | while read -r line; do
    echo "rmt_fetch	check	$line"
done

##########################################################################################
# The write side re-validates what admission assumed. Every other case here holds the copy mode
# constant from CREATE through mutation, so the check that a mutation admitted as hardlink-only is
# still hardlink-only when it reaches the write side is never exercised: delete it and nothing goes
# red. This case changes the mode WHILE the entry is paused between admission and the write, which is
# the only window in which the two can disagree.
#
# The refusal must also be recoverable: the second arm restores the mode and the same entry completes,
# which is what makes it a postpone rather than a permanently poisoned queue entry.
##########################################################################################

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE rmt_copy_flip (event String, id UInt64,
        INDEX idx_event event TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/rmt_copy_flip/', '1') ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             packed_skip_index_max_bytes = 0, always_use_copy_instead_of_hardlinks = 0,
             merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

    INSERT INTO rmt_copy_flip SELECT repeat('abcdefgh', 20), number FROM numbers(20000);
    OPTIMIZE TABLE rmt_copy_flip FINAL;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_mutate_task_pause_in_prepare;
    ALTER TABLE rmt_copy_flip DROP INDEX idx_event SETTINGS alter_sync = 0;
    SYSTEM WAIT FAILPOINT rmt_mutate_task_pause_in_prepare PAUSE;
"

# Paused between admission and the write: flip the mode the entry was admitted under, then let it go.
$CLICKHOUSE_CLIENT --query "
    ALTER TABLE rmt_copy_flip MODIFY SETTING always_use_copy_instead_of_hardlinks = 1;
    SYSTEM NOTIFY FAILPOINT rmt_mutate_task_pause_in_prepare;
"

# It must be refused rather than complete: completing would copy the retained files, using space it
# only ever reserved a fraction of. Only the error name is asserted, not the message text.
for _ in $(seq 1 300); do
    result=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = 'rmt_copy_flip'
          AND position(latest_fail_reason, 'NOT_ENOUGH_SPACE') > 0
    ")
    if [ "$result" -gt 0 ]; then
        break
    fi
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
    SELECT 'rmt_copy_flip', 'refused', countIf(position(latest_fail_reason, 'NOT_ENOUGH_SPACE') > 0),
        'not_done', countIf(NOT is_done)
    FROM system.mutations WHERE database = currentDatabase() AND table = 'rmt_copy_flip';
"

# Restoring the mode must let the same entry through: the refusal is a postpone, not a poisoning.
$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_in_prepare;
    ALTER TABLE rmt_copy_flip MODIFY SETTING always_use_copy_instead_of_hardlinks = 0;
"

wait_for_mutation "rmt_copy_flip" "0000000000"

$CLICKHOUSE_CLIENT --query "
    SELECT 'rmt_copy_flip_retry', 'pending', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'rmt_copy_flip' AND NOT is_done;

    SELECT 'rmt_copy_flip_retry', 'indices_left', count() FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 'rmt_copy_flip';

    SELECT 'rmt_copy_flip_retry', 'rows', count() FROM rmt_copy_flip;
"
$CLICKHOUSE_CLIENT --query "CHECK TABLE rmt_copy_flip" | while read -r line; do
    echo "rmt_copy_flip_retry	check	$line"
done

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_max_part_size;
    DROP TABLE rmt_drop_index SYNC;
    DROP TABLE rmt_delete SYNC;
    DROP TABLE rmt_fetch_1 SYNC;
    DROP TABLE rmt_fetch_2 SYNC;
    DROP TABLE rmt_copy_flip SYNC;
"

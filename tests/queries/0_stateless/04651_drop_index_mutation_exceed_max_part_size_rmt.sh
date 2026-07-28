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

# The failpoint is server-wide, so leaving it enabled after an early exit under `set -e` would give
# every later mutation on this server a 1 byte budget.
trap '$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_max_part_size;
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

for _ in $(seq 1 300); do
    result=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = 'rmt_delete'
          AND NOT is_done AND notEmpty(parts_postpone_reasons)
    ")
    if [ "$result" -gt 0 ]; then
        break
    fi
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
    SELECT 'rmt_delete', 'postponed',
        arrayExists(reason -> reason = 'Exceed max source part size', mapValues(parts_postpone_reasons)),
        'no_failure', empty(latest_fail_reason)
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 'rmt_delete' AND NOT is_done;
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_max_part_size;
    DROP TABLE rmt_drop_index SYNC;
    DROP TABLE rmt_delete SYNC;
"

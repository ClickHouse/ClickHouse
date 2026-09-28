#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database, no-parallel-replicas
# The failpoint is global, so this test cannot overlap another failpoint test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT smt_merge_selecting_task_pause_when_scheduled" >/dev/null 2>&1 || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS smt_row_mask_coalesce SYNC" >/dev/null 2>&1 || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;
    DROP TABLE IF EXISTS smt_row_mask_coalesce SYNC;
    CREATE TABLE smt_row_mask_coalesce
    (
        n UInt64,
        project_id UInt32,
        run_id UInt128,
        payload UInt8
    )
    ENGINE = SharedMergeTree('/zookeeper/{database}/smt_row_mask_coalesce/', '1')
    ORDER BY (project_id, run_id, n)
    SETTINGS storage_policy = 's3_with_keeper',
        min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0;
    INSERT INTO smt_row_mask_coalesce VALUES
        (1, 1, 1, 0), (2, 1, 2, 0), (3, 1, 3, 0),
        (4, 2, 1, 0), (5, 2, 2, 0), (6, 2, 3, 0),
        (7, 3, 1, 0);
"
source_part=$($CLICKHOUSE_CLIENT --query "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 'smt_row_mask_coalesce' AND active
    LIMIT 1")
[[ -n $source_part ]]

$CLICKHOUSE_CLIENT --query "
    SET allow_nondeterministic_mutations = 1;
    SYSTEM ENABLE FAILPOINT smt_merge_selecting_task_pause_when_scheduled;
    SYSTEM WAIT FAILPOINT smt_merge_selecting_task_pause_when_scheduled PAUSE;
    ALTER TABLE smt_row_mask_coalesce UPDATE _row_exists = 0
        WHERE project_id = 1 AND run_id IN (SELECT toUInt128(arrayJoin(['1'])));
    ALTER TABLE smt_row_mask_coalesce UPDATE _row_exists = 0
        WHERE project_id = 2 AND run_id IN (SELECT toUInt128(arrayJoin(['1'])));
    ALTER TABLE smt_row_mask_coalesce UPDATE _row_exists = 0
        WHERE project_id = 1 AND run_id IN (SELECT toUInt128(arrayJoin(['2'])));
    ALTER TABLE smt_row_mask_coalesce UPDATE payload = 9 WHERE n = 6;
    ALTER TABLE smt_row_mask_coalesce UPDATE _row_exists = 0
        WHERE project_id = 1 AND run_id IN (SELECT toUInt128(arrayJoin(['3'])));
    ALTER TABLE smt_row_mask_coalesce UPDATE _row_exists = 0
        WHERE project_id = 2 AND run_id IN (SELECT toUInt128(arrayJoin(['2'])));
"

mutation_count=0
for _ in {1..300}; do
    mutation_count=$($CLICKHOUSE_CLIENT --query="SELECT count() FROM system.mutations WHERE database='$CLICKHOUSE_DATABASE' AND table='smt_row_mask_coalesce'")
    if [[ $mutation_count -eq 6 ]]; then
        break
    fi
    sleep 0.1
done
if [[ $mutation_count -ne 6 ]]; then
    echo "Expected 6 queued mutations, got $mutation_count" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT smt_merge_selecting_task_pause_when_scheduled"
wait_for_mutation "smt_row_mask_coalesce" "0000000005"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS part_log, text_log"

$CLICKHOUSE_CLIENT --query "
    SELECT n, project_id, run_id, payload FROM smt_row_mask_coalesce ORDER BY n;
    SELECT count(), countIf(is_done) FROM system.mutations
        WHERE database = currentDatabase() AND table = 'smt_row_mask_coalesce';
    SELECT count() FROM system.part_log
        WHERE database = currentDatabase() AND table = 'smt_row_mask_coalesce'
        AND event_type = 'MutatePart'
        AND hasAll(mutation_ids, ['0000000000', '0000000001', '0000000002',
            '0000000003', '0000000004', '0000000005']);
    SELECT count() > 0 FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND logger_name = 'MutateTask'
        AND message = concat('Coalesced row-mask updates for part ', '$source_part',
            ' (mutation commands 6 -> 3)');
"

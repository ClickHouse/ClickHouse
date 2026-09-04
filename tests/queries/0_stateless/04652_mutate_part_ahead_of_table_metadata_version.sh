#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
# Tag no-parallel: replicated_queue_fail_next_entry is a server-global ONCE failpoint, so a
#                  concurrent copy of this test would consume it instead of our ALTER_METADATA
# Tag no-shared-merge-tree: tests ReplicatedMergeTree queue behaviour with failpoints
# Tag no-replicated-database: the test drives ALTER_METADATA on individual replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

function restore_failpoints()
{
    if [ -z "${held_replica:-}" ]; then
        return
    fi

    # Let the failed ALTER_METADATA succeed again, to avoid endless errors in logs
    $CLICKHOUSE_CLIENT -q "system enable failpoint replicated_queue_unfail_entries" ||:
    $CLICKHOUSE_CLIENT -q "system sync replica $held_replica" ||:
    $CLICKHOUSE_CLIENT -q "system disable failpoint replicated_queue_unfail_entries" ||:
}
trap restore_failpoints EXIT

$CLICKHOUSE_CLIENT -m --insert_keeper_fault_injection_probability=0 -q "
    drop table if exists data_r1;
    drop table if exists data_r2;

    -- Merges disabled on both replicas: the pre-ALTER and post-ALTER parts are mutually mergeable,
    -- and a covering part would make the exact-name assertions below vacuous.
    create table data_r1 (key Int, value Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/data', 'r1') order by key settings max_bytes_to_merge_at_max_space_in_pool = 0;
    create table data_r2 (key Int, value Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/data', 'r2') order by key settings max_bytes_to_merge_at_max_space_in_pool = 0;

    insert into data_r1 (key, value) values (1, 10);

    -- Sync both replicas before enabling the failpoint, so no pending queue entry
    -- (e.g. GET_PART for data_r2) can consume the ONCE failpoint instead of ALTER_METADATA.
    system sync replica data_r1;
    system sync replica data_r2;
"

# Fail ALTER_METADATA on one of the replicas. That replica keeps the entry in its queue and stays
# at metadata version 0, while the other one advances to metadata version 1.
$CLICKHOUSE_CLIENT -m -q "
    system enable failpoint replicated_queue_fail_next_entry;
    alter table data_r1 add column h Int default 7 settings alter_sync = 0;

    system sync replica data_r1 pull;
    system sync replica data_r2 pull;
"

# Which replica consumes the ONCE failpoint is not fixed, so measure it. ADD COLUMN is
# metadata-only and creates no mutation, so system.mutations cannot tell the replicas apart --
# system.tables.metadata_version is the quantity that actually differs.
held_replica=
advanced_replica=
for ((i = 0; i < 300; ++i)); do
    mv1=$($CLICKHOUSE_CLIENT -q "select metadata_version from system.tables where database = currentDatabase() and name = 'data_r1'")
    mv2=$($CLICKHOUSE_CLIENT -q "select metadata_version from system.tables where database = currentDatabase() and name = 'data_r2'")
    if [[ $mv1 -gt $mv2 ]]; then
        held_replica=data_r2 && advanced_replica=data_r1 && break
    fi
    if [[ $mv2 -gt $mv1 ]]; then
        held_replica=data_r1 && advanced_replica=data_r2 && break
    fi
    sleep 0.1
done
if [[ -z $held_replica ]]; then
    echo "Table metadata version did not diverge between the replicas" >&2 && exit 1
fi

# Write a part at metadata version 1 containing `h` and let the held-back replica fetch it.
# metadata_version.txt travels with the part, so the fetched part is ahead of that replica's table.
# Two rows, so the DELETE below touches some but not all of them: a mutation that affects no row
# (or every row) is short-circuited before the mutation commands are computed at all.
$CLICKHOUSE_CLIENT -m --insert_keeper_fault_injection_probability=0 -q "
    insert into $advanced_replica (key, value, h) values (2, 20, 99), (3, 30, 98);
    system sync replica $advanced_replica pull;
"

for ((i = 0; i < 300; ++i)); do
    fetched=$($CLICKHOUSE_CLIENT -q "select count() from system.parts where database = currentDatabase() and table = '$held_replica' and active and name = 'all_1_1_0'")
    if [[ $fetched -eq 1 ]]; then
        break
    fi
    sleep 0.1
done

# Assert the fixture really built the state, otherwise the test below proves nothing.
echo -n 'held replica still at metadata version 0: '
$CLICKHOUSE_CLIENT -q "select metadata_version = 0 from system.tables where database = currentDatabase() and name = '$held_replica'"
echo -n 'other replica advanced to metadata version 1: '
$CLICKHOUSE_CLIENT -q "select metadata_version = 1 from system.tables where database = currentDatabase() and name = '$advanced_replica'"
echo -n 'ALTER_METADATA still queued on the held replica: '
$CLICKHOUSE_CLIENT -q "select count() = 1 from system.replication_queue where database = currentDatabase() and table = '$held_replica' and type = 'ALTER_METADATA' and num_tries > 0"
echo -n 'held replica fetched the part containing h: '
$CLICKHOUSE_CLIENT -q "select count() = 1 from system.parts_columns where database = currentDatabase() and table = '$held_replica' and name = 'all_1_1_0' and column = 'h'"

# A plain data mutation carries alter_version = -1, so the existing alter sequencing does not
# apply to it. Before the fix this mutated the fetched part against a table snapshot that does not
# know `h` and threw LOGICAL_ERROR "Part ... contains column h that is absent in table ...".
$CLICKHOUSE_CLIENT -q "alter table $held_replica delete where key = 2 settings mutations_sync = 0"

# The MUTATE_PART entry has to be postponed instead. Asserting the postpone reason (and not just
# the absence of an error) is what proves the guard fired.
postponed=0
for ((i = 0; i < 300; ++i)); do
    postponed=$($CLICKHOUSE_CLIENT -q "select count() from system.replication_queue where database = currentDatabase() and table = '$held_replica' and type = 'MUTATE_PART' and postpone_reason like '%metadata version 1 is newer than the table metadata version 0%'")
    if [[ $postponed -eq 1 ]]; then
        break
    fi
    sleep 0.1
done
echo -n 'MUTATE_PART postponed because the source part is ahead of the table: '
echo "$postponed"

# Applying the pending ALTER_METADATA resolves the mismatch and the mutation goes through.
restore_failpoints
trap '' EXIT
held_replica=

$CLICKHOUSE_CLIENT -m -q "
    system sync replica data_r1;
    system sync replica data_r2;
"

echo -n 'both replicas reached metadata version 1: '
$CLICKHOUSE_CLIENT -q "select countIf(metadata_version = 1) = 2 from system.tables where database = currentDatabase() and name in ('data_r1', 'data_r2')"
echo -n 'mutation finished without a failure: '
$CLICKHOUSE_CLIENT -q "select countIf(is_done and latest_fail_reason = '') = 2 from system.mutations where database = currentDatabase() and table in ('data_r1', 'data_r2')"
echo 'data on data_r1:'
$CLICKHOUSE_CLIENT -q "select key, value, h from data_r1 order by key"
echo 'data on data_r2:'
$CLICKHOUSE_CLIENT -q "select key, value, h from data_r2 order by key"

$CLICKHOUSE_CLIENT -m -q "
    drop table data_r1;
    drop table data_r2;
"

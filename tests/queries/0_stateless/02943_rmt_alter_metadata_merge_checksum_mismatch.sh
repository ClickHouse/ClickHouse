#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: failpoint is in use

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

function wait_part()
{
    local table=$1 && shift
    local part=$1 && shift

    for ((i = 0; i < 100; ++i)); do
        if [[ $($CLICKHOUSE_CLIENT -q "select count() from system.parts where database = '$CLICKHOUSE_DATABASE' and table = '$table' and active and name = '$part'") -eq 1 ]]; then
            return
        fi
        sleep 0.1
    done

    echo "Part $table::$part does not appeared" >&2
}

function restore_failpoints()
{
    # restore entry error with failpoints (to avoid endless errors in logs)
    $CLICKHOUSE_CLIENT -m -q "
        system enable failpoint replicated_queue_unfail_entries;
        system sync replica $failed_replica;
        system disable failpoint replicated_queue_unfail_entries;
    "
}
trap restore_failpoints EXIT

$CLICKHOUSE_CLIENT -m --insert_keeper_fault_injection_probability=0 -q "
    drop table if exists data_r1;
    drop table if exists data_r2;

    create table data_r1 (key Int, value Int, index value_idx value type minmax) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/data', '{table}') order by key;
    create table data_r2 (key Int, value Int, index value_idx value type minmax) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/data', '{table}') order by key;

    insert into data_r1 (key) values (1); -- part all_0_0_0
"

# will fail ALTER_METADATA on one of replicas
$CLICKHOUSE_CLIENT -m -q "
    system enable failpoint replicated_queue_fail_next_entry;
    alter table data_r1 drop index value_idx settings alter_sync=0; -- part all_0_0_0_1

    system sync replica data_r1 pull;
    system sync replica data_r2 pull;
"

# replica on which ALTER_METADATA had been succeed
success_replica=
for ((i = 0; i < 100; ++i)); do
    for table in data_r1 data_r2; do
        mutations="$($CLICKHOUSE_CLIENT -q "select count() from system.mutations where database = '$CLICKHOUSE_DATABASE' and table = '$table' and is_done = 0")"
        if [[ $mutations -eq 0 ]]; then
            success_replica=$table
        fi
    done
    if [[ -n $success_replica ]]; then
        break
    fi
    sleep 0.1
done
case "$success_replica" in
    data_r1) failed_replica=data_r2;;
    data_r2) failed_replica=data_r1;;
    *) echo "ALTER_METADATA does not succeed on any replica" >&2 && exit 1;;
esac
mutations_on_failed_replica="$($CLICKHOUSE_CLIENT -q "select count() from system.mutations where database = '$CLICKHOUSE_DATABASE' and table = '$failed_replica' and is_done = 0")"
if [[ $mutations_on_failed_replica != 1 ]]; then
    echo "Wrong number of mutations on failed replica $failed_replica, mutations $mutations_on_failed_replica" >&2
fi

# This will create MERGE_PARTS, on failed replica it will be fetched from source replica (since it does not have all parts to execute merge)
$CLICKHOUSE_CLIENT -q "optimize table $success_replica final settings optimize_throw_if_noop=1, alter_sync=1" # part all_0_0_1_1

$CLICKHOUSE_CLIENT -m --insert_keeper_fault_injection_probability=0 -q "
    insert into $success_replica (key) values (2); -- part all_2_2_0
    -- Avoid 'Cannot select parts for optimization: Entry for part all_2_2_0 hasn't been read from the replication log yet'
    system sync replica $success_replica pull;
    optimize table $success_replica final settings optimize_throw_if_noop=1, alter_sync=1; -- part all_0_2_2_1
    system sync replica $failed_replica pull;
"

# Wait for part to be merged on failed replica, that will trigger CHECKSUM_DOESNT_MATCH
wait_part "$failed_replica" all_0_2_2_1

# Already after part fetched there will CHECKSUM_DOESNT_MATCH in case of ALTER_METADATA re-order, but let's restore fail points and sync failed replica first.
restore_failpoints
trap '' EXIT

$CLICKHOUSE_CLIENT -q "system flush logs"
# check for error "Different number of files: 5 compressed (expected 3) and 2 uncompressed ones (expected 2). (CHECKSUM_DOESNT_MATCH)"
$CLICKHOUSE_CLIENT -q "select part_name, merge_reason, event_type, errorCodeToName(error) from system.part_log where database = '$CLICKHOUSE_DATABASE' and error != 0 and errorCodeToName(error) != 'NO_REPLICA_HAS_PART' order by event_time_microseconds"

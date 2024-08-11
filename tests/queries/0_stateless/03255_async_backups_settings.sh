#!/usr/bin/env bash

# Ensure that mmap will not be used for reading (it does not support throttling)
CLICKHOUSE_CLIENT_OPT0="--min_bytes_to_use_mmap_io=0"

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function wait_backup_by_query_id()
{
    local query_id="$1" && shift
    local expected_status="$1"
    local timeout=60
    local start=$EPOCHSECONDS
    while true; do
        local current_status
        current_status=$($CLICKHOUSE_CLIENT --query "SELECT status FROM system.backups WHERE query_id = '$query_id'")
        if [ "${current_status}" == "${expected_status}" ]; then
            echo "${current_status}"
            break
        fi
        if ((EPOCHSECONDS-start > timeout )); then
            echo "Timeout while waiting for $query_id to come to status ${expected_status}. The current status is ${current_status}."
            exit 1
        fi
        $CLICKHOUSE_CLIENT --query "SELECT current_database, Settings['max_backup_bandwidth'] FROM system.processes WHERE query_id = '$query_id'"
        sleep 0.1
    done
}

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists data;
    create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9;
    -- reading 1e6*8 bytes with 1M bandwith it should take (8-1)/1=7 seconds
    insert into data select * from numbers(1e6);
"

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "backup table data to $backup_name ASYNC" --max_backup_bandwidth=1M > /dev/null
wait_backup_by_query_id "$query_id" BACKUP_CREATED | sort | uniq

query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "restore table data as data_new from $backup_name ASYNC" --max_backup_bandwidth=1M > /dev/null
wait_backup_by_query_id "$query_id" RESTORED | sort | uniq

#!/usr/bin/env bash
# Tags: no-fasttest
# Tag: no-fasttest - requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function wait_restores()
{
    local name=$1
    local timeout=60
    local start=$EPOCHSECONDS
    while true; do
        local n
        n=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.backups WHERE name = {name:String} AND status = 'RESTORING'" --param_name "$name")
        if [[ $n -eq 0 ]]; then
            break
        fi
        if (( EPOCHSECONDS - start > timeout )); then
            echo "Timeout while waiting for RESTOREs. Currently running restores: $n"
            return 1
        fi
        sleep 0.1
    done

    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.backups WHERE name = {name:String} AND status = 'RESTORED'" --param_name "$name"
}

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
$CLICKHOUSE_CLIENT --format Null -q "BACKUP TABLE system.users TO $backup_name ASYNC"

# We need some fast BACKUP/RESTORE that will finishes before PipelineExecutor creation
yes "RESTORE TABLE system.users FROM $backup_name ASYNC;" | head -n 1000 | $CLICKHOUSE_CLIENT -n --format Null

wait_restores "$backup_name"

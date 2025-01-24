#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-parallel
# Tag no-fasttest: requires S3
# Tag no-shared-merge-tree: does not support replication
# Tag no-parallel: uses failpoints

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./parts.lib
. "$CUR_DIR"/parts.lib

on_exit() {
    ${CLICKHOUSE_CLIENT} -m --query "
    SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_create;
    SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_move;
"
}

trap on_exit EXIT

REMOVAL_STATE_CONDITION="(
    removal_state='Part was selected to be removed but then it had been rollbacked. The remove will be retried.'
    OR removal_state='Retry to remove part.')"

function wait_for_part_remove_rollbacked()
{
    local table=$1
    local database=${2:-$CLICKHOUSE_DATABASE}
    local timeout=${3:-20}

    local query="
        SELECT count() > 0 FROM system.parts
        WHERE database='$database' AND table='$table' AND not active
        AND $REMOVAL_STATE_CONDITION"

    while [[ timeout -gt 0 ]]
    do
        res=$(${CLICKHOUSE_CLIENT} --query="$query")
        [[ $res -eq 1 ]] && return 0

        sleep 2
        timeout=$((timeout - 2))
    done

    echo "Timed out while waiting for part remove is rollbacked" >&2
    return 2
}

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_s3_mt_fault"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE test_s3_mt_fault (a Int32, b Int64) engine = MergeTree() ORDER BY tuple(a, b)
SETTINGS
    disk = disk(
        name = 03008_s3_plain_rewritable_fault,
        type = s3_plain_rewritable,
        endpoint = 'http://localhost:11111/test/03008_test_s3_mt_fault/',
        access_key_id = clickhouse,
        secret_access_key = clickhouse),
    old_parts_lifetime = 1;
"

${CLICKHOUSE_CLIENT} --query "
INSERT INTO test_s3_mt_fault (*) VALUES (1, 2), (2, 2), (3, 1), (4, 7), (5, 10), (6, 12);
OPTIMIZE TABLE test_s3_mt_fault FINAL;
"


${CLICKHOUSE_CLIENT} --query "
SYSTEM ENABLE FAILPOINT plain_object_storage_write_fail_on_directory_create
"
${CLICKHOUSE_CLIENT} --query "
INSERT INTO test_s3_mt_fault (*) select number, number from numbers_mt(100)
" 2>&1 | grep -Fq "FAULT_INJECTED"
${CLICKHOUSE_CLIENT} --query "
SELECT * FROM test_s3_mt_fault;
"
${CLICKHOUSE_CLIENT} --query "
SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_create;
"


${CLICKHOUSE_CLIENT} --query "
SYSTEM ENABLE FAILPOINT plain_object_storage_write_fail_on_directory_move;
"
${CLICKHOUSE_CLIENT} --query "
INSERT INTO test_s3_mt_fault (*) select number, number from numbers_mt(100);
" 2>&1 | grep -Fq "FAULT_INJECTED"

${CLICKHOUSE_CLIENT} --query "
SELECT * FROM test_s3_mt_fault;"

${CLICKHOUSE_CLIENT} --query "
SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_move;
"


# cheche that parts aren't stuck in Deleting state when excpetion at patrs remove occurs
active_count=$(${CLICKHOUSE_CLIENT} --query "
select countIf(active) from system.parts
where database = '${CLICKHOUSE_DATABASE}' and table = 'test_s3_mt_fault'")
[[ $active_count -gt 0 ]] || echo "At least one active part is expected"

${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test_s3_mt_fault;"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT plain_object_storage_write_fail_on_directory_move;"

inactive_count=$(${CLICKHOUSE_CLIENT} --query "
select countIf(active = 0) from system.parts
where database = '${CLICKHOUSE_DATABASE}' and table = 'test_s3_mt_fault'")
[[ $inactive_count -gt 0 ]] || echo "At least one inactive part is expected"

wait_for_part_remove_rollbacked test_s3_mt_fault

inactive_count=$(${CLICKHOUSE_CLIENT} --query "
select count() from system.parts
where database = '${CLICKHOUSE_DATABASE}' and table = 'test_s3_mt_fault' and $REMOVAL_STATE_CONDITION")
[[ $inactive_count -gt 0 ]] || echo "At least one inactive part which has been rollbacked from remove is expected"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_move;"

timeout 60 bash -c 'wait_for_delete_inactive_parts test_s3_mt_fault'

${CLICKHOUSE_CLIENT} --query "select 'there shoud be no rows', *, _state from system.parts where database = '${CLICKHOUSE_DATABASE}' and table = 'test_s3_mt_fault'"

# Filter out 'Removing temporary directory' because the fault injection prevents directory rename.
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_s3_mt_fault SYNC" 2>&1 | grep -v 'Removing temporary directory' ||:

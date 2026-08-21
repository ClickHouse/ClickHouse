#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-parallel, no-replicated-database
# Tag no-fasttest: requires S3
# Tag no-shared-merge-tree: does not support replication
# Tag no-parallel: uses failpoints
# Tag no-replicated-database: plain rewritable should not be shared between replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./parts.lib
. "$CUR_DIR"/parts.lib

on_exit() {
    ${CLICKHOUSE_CLIENT} -m --query "
    SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_create;
    SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_move;
    SYSTEM DISABLE FAILPOINT storage_merge_tree_background_clear_old_parts_pause;
"
}

trap on_exit EXIT

set -eu

REMOVAL_STATE_CONDITION="(
    removal_state='Part was selected to be removed but then it had been rolled back. The remove will be retried'
    OR removal_state='Retry to remove part')"

STATE_CONDITION="_state in ['Deleting', 'Outdated']"

function wait_for_part_remove_rolled_back()
{
    local table=$1
    local database=${2:-$CLICKHOUSE_DATABASE}
    local timeout=${3:-20}

    local query="
        SELECT count() > 0 FROM system.parts
        WHERE database='$database' AND table='$table'
        AND $STATE_CONDITION
        AND $REMOVAL_STATE_CONDITION"

    while [[ timeout -gt 0 ]]
    do
        res=$(${CLICKHOUSE_CLIENT} --query="$query")
        [[ $res -eq 1 ]] && return 0

        sleep 2
        timeout=$((timeout - 2))
    done

    echo "Timed out while waiting for part remove is rolled back" >&2
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


# Check that parts aren't stuck in Deleting state when exception at parts remove occurs

# It is important to select _state column from system.parts,
# otherwise parts with Deleting state are omitted in the select results

active_count=$(${CLICKHOUSE_CLIENT} --query "
select countIf(active) from system.parts
where database = '${CLICKHOUSE_DATABASE}' and table = 'test_s3_mt_fault'")
if [[ $active_count -eq 0 ]]
then
    echo "At least one active part is expected"
    exit 2
fi

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT storage_merge_tree_background_clear_old_parts_pause;"

${CLICKHOUSE_CLIENT} --query "
INSERT INTO test_s3_mt_fault (*) VALUES (1, 2), (2, 2), (3, 1), (4, 7), (5, 10), (6, 12);
OPTIMIZE TABLE test_s3_mt_fault FINAL;
"

# Now there is one inactive part, that is not cleaned up yet.

inactive_count=$(${CLICKHOUSE_CLIENT} --query "
select count() from system.parts
where database = '${CLICKHOUSE_DATABASE}' and table = 'test_s3_mt_fault' and $STATE_CONDITION")
if [[ $inactive_count -eq 0 ]]
then
    echo "At least one inactive part is expected"
    exit 2
fi

# Now we enable the cleanup, but also make an exception while attempting to remove the directory
# (all directories are removed via moving first)

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT plain_object_storage_write_fail_on_directory_move;"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT storage_merge_tree_background_clear_old_parts_pause;"

wait_for_part_remove_rolled_back test_s3_mt_fault

inactive_count=$(${CLICKHOUSE_CLIENT} --query "
select count() from system.parts
where database = '${CLICKHOUSE_DATABASE}' and table = 'test_s3_mt_fault' and $STATE_CONDITION and $REMOVAL_STATE_CONDITION")
if [[ $inactive_count -eq 0 ]]
then
    echo "At least one inactive part which has been rolled back from remove is expected"
    exit 2
fi

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_move;"

timeout 60 bash -c 'wait_for_delete_inactive_parts test_s3_mt_fault'

# Filter out 'Removing temporary directory' because the fault injection prevents directory rename.
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_s3_mt_fault SYNC" 2>&1 | grep -v 'Removing temporary directory' ||:

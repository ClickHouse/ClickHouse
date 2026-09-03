#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS tbl;
DROP TABLE IF EXISTS tbl2;
CREATE TABLE tbl (a Int32) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO tbl VALUES (2), (80), (-12345);
"

function start_async()
{
    local command="$1"
    local first_column="s/^\([^\t]*\)\t.*/\1/"
    ${CLICKHOUSE_CLIENT} --query "$command" | sed "${first_column}"
}

function wait_status()
{
    local operation_id="$1"
    local expected_status="$2"
    local timeout=60
    local start=$EPOCHSECONDS
    while true; do
        local current_status
        current_status=$(${CLICKHOUSE_CLIENT} --query "SELECT status FROM system.backups WHERE id='${operation_id}'")
        if [ "${current_status}" == "${expected_status}" ]; then
            echo "${current_status}"
            break
        fi
        if ((EPOCHSECONDS-start > timeout )); then
            echo "Timeout while waiting for operation ${operation_id} to come to status ${expected_status}. The current status is ${current_status}."
            exit 1
        fi
        sleep 0.1
    done
}

# Making a backup.
backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
backup_operation_id=$(start_async "BACKUP TABLE tbl TO ${backup_name} ASYNC")
wait_status "${backup_operation_id}" "BACKUP_CREATED"

# Restoring from that backup.
restore_operation_id=$(start_async "RESTORE TABLE tbl AS tbl2 FROM ${backup_name} ASYNC")
wait_status "${restore_operation_id}" "RESTORED"

# Check the result of that restoration.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM tbl2"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE tbl;
DROP TABLE tbl2;
"

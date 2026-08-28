#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Formatting a query cannot show whether a `name = DEFAULT` item was actually resolved: the formatter
# prints the two SETTINGS carriers and never consults BackupSettings/RestoreSettings. So assert the
# resolved value itself, through system.backups.settings, which reports what the settings layer built.

${CLICKHOUSE_CLIENT} -m --query "
drop table if exists src;
create table src (a Int32) engine = MergeTree() order by tuple();
insert into src select * from numbers(10);
"

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
uniq="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Copied from 03032_async_backup_restore.sh: an async operation must not be left racing the DROPs below.
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
            break
        fi
        if ((EPOCHSECONDS-start > timeout )); then
            echo "Timeout while waiting for operation ${operation_id} to come to status ${expected_status}. The current status is ${current_status}."
            exit 1
        fi
        sleep 0.1
    done
}

${CLICKHOUSE_CLIENT} --query "backup table src to $backup_name settings id='${uniq}_b0'" | grep -o "BACKUP_CREATED"

echo "-- RESTORE: a restore-specific reset must be delivered, not merely accepted."

# Control: the override lands, so the probe is wired up and 'structure_only' really reaches RestoreSettings.
${CLICKHOUSE_CLIENT} --query "
restore table src as r1 from $backup_name settings id='${uniq}_r1', structure_only=1;
" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "
select settings['structure_only'], (select count() from r1) from system.backups where id='${uniq}_r1'
"

# The reset puts the field back to its default, so the rows are restored after all.
${CLICKHOUSE_CLIENT} --query "
restore table src as r2 from $backup_name settings id='${uniq}_r2', structure_only=1, structure_only=DEFAULT;
" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "
select settings['structure_only'], (select count() from r2) from system.backups where id='${uniq}_r2'
"

# The reported bug shape: a leading unknown `= DEFAULT` item used to divert the whole clause.
${CLICKHOUSE_CLIENT} --query "
restore table src as r3 from $backup_name
settings id='${uniq}_r3', foo=DEFAULT, structure_only=1, structure_only=DEFAULT;
" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "
select settings['structure_only'], (select count() from r3) from system.backups where id='${uniq}_r3'
"

echo "-- Alias pairs address one field, so defaulting either spelling must drop the other's override."

# Controls: both spellings write the same field.
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_a1') settings id='${uniq}_a1', s3_storage_class='STANDARD_IA';
" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_a2') settings id='${uniq}_a2', s3_storage_class_name='STANDARD_IA';
" | grep -o "BACKUP_CREATED"

# Either spelling defaulted drops an override written as the other.
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_a3')
settings id='${uniq}_a3', s3_storage_class='STANDARD_IA', s3_storage_class_name=DEFAULT;
" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_a4')
settings id='${uniq}_a4', s3_storage_class_name='STANDARD_IA', s3_storage_class=DEFAULT;
" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} --query "
select id like '%_a1' or id like '%_a2' as is_control, settings['s3_storage_class']
from system.backups where id in ('${uniq}_a1', '${uniq}_a2', '${uniq}_a3', '${uniq}_a4') order by id
"

# The RESTORE twin: an obsolete name and its current name are one setting.
${CLICKHOUSE_CLIENT} --query "
restore table src as r4 from $backup_name
settings id='${uniq}_r4', structure_only=1, allow_unresolved_access_dependencies=1;
" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "
restore table src as r5 from $backup_name
settings id='${uniq}_r5', structure_only=1, allow_unresolved_access_dependencies=1, skip_unresolved_access_dependencies=DEFAULT;
" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "
select id like '%_r4' as is_control, settings['skip_unresolved_access_dependencies']
from system.backups where id in ('${uniq}_r4', '${uniq}_r5') order by id
"

echo "-- \`async\` decides whether the client waits, so resetting it must change the returned status."

# `BackupSettings::isAsync` resolves `async = DEFAULT` on its own (it runs before `fromBackupQuery`) and is
# the sole input to InterpreterBackupQuery's decision whether to wait. Reset back to the default (false)
# => the interpreter waits => the query itself reports the finished status.
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_s1')
settings id='${uniq}_s1', async=1, async=DEFAULT;
" | grep -o "BACKUP_CREATED"

# Control proving that arm is not vacuous: with `async=1` alone the query returns without waiting.
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_s2') settings id='${uniq}_s2', async=1;
" | grep -o "CREATING_BACKUP"
wait_status "${uniq}_s2" "BACKUP_CREATED"

# A repeated `async` takes its last value, so the wait decision must follow the effective setting.
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_s3') settings id='${uniq}_s3', async=0, async=1;
" | grep -o "CREATING_BACKUP"
wait_status "${uniq}_s3" "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_s4') settings id='${uniq}_s4', async=1, async=0;
" | grep -o "BACKUP_CREATED"

# A string value converts as the Bool setting field does, rather than aborting the query.
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_s5') settings id='${uniq}_s5', async='1';
" | grep -o "CREATING_BACKUP"
wait_status "${uniq}_s5" "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} --query "
select id like '%_s4' as expected_sync, settings['async']
from system.backups where id in ('${uniq}_s3', '${uniq}_s4', '${uniq}_s5') order by id
"

echo "-- \`compression\` has no effect from a SETTINGS clause, so both forms are refused here too."

# Refused for the same reason as on an ordinary query: the HTTP response body is shaped before the
# query runs. `compression_method` is a BACKUP setting and a different name, so it keeps working.
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_h1') settings compression = 'gz';
" 2>&1 | grep -m1 -o "shapes the HTTP response body"
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_h2') settings compression = DEFAULT;
" 2>&1 | grep -m1 -o "shapes the HTTP response body"
${CLICKHOUSE_CLIENT} --query "
restore table src as h3 from $backup_name settings compression = DEFAULT;
" 2>&1 | grep -m1 -o "shapes the HTTP response body"

# Control: the BACKUP-specific compression settings are unaffected, in both forms.
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_h4') settings compression_method = 'lz4', compression_level = DEFAULT;
" | grep -o "BACKUP_CREATED"

echo "-- \`base_backup\` keeps the meaning each clause gives it today: a backup name on its own, a reset"
echo "-- once the clause holds a defaulted item, which is where the field is cleared."

# A bare identifier is a backup name, `DEFAULT` included, and that reading is reachable on its own.
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_b1') settings base_backup = DEFAULT;
" 2>&1 | grep -m1 -o "BACKUP_ENGINE_NOT_FOUND"
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_b2') settings foo = DEFAULT, base_backup = DEFAULT;
" | grep -o "BACKUP_CREATED"

# Control: a locator is still read as one next to a defaulted sibling.
${CLICKHOUSE_CLIENT} --query "
backup table src to Disk('backups', '${uniq}_b3')
settings foo = DEFAULT, base_backup = Disk('backups', '${uniq}_h4');
" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} -m --query "
drop table r1; drop table r2; drop table r3; drop table r4; drop table r5; drop table src;
"

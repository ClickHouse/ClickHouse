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

${CLICKHOUSE_CLIENT} -m --query "
drop table r1; drop table r2; drop table r3; drop table r4; drop table r5; drop table src;
"

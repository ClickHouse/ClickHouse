#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
drop table if exists \`04409_t\`;
create table \`04409_t\` (a Int32) engine = MergeTree() order by tuple();
insert into \`04409_t\` select * from numbers(10);
"

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')";
restore_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_restore";

${CLICKHOUSE_CLIENT} --query "backup table ${CLICKHOUSE_DATABASE}.\`04409_t\` to $backup_name" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} --query "drop table ${CLICKHOUSE_DATABASE}.\`04409_t\`"

# Restore with a few non-default restore-specific settings.
${CLICKHOUSE_CLIENT} --query "
restore table ${CLICKHOUSE_DATABASE}.\`04409_t\` from $backup_name
settings id='$restore_id', structure_only=1, restore_broken_parts_as_detached=1, allow_non_empty_tables=1;
" | grep -o "RESTORED"

# The restore-specific settings are observable in system.backups.settings, while the secret
# 'password' and the duplicative 'id' are never exposed.
${CLICKHOUSE_CLIENT} -m --query "
select
    settings['structure_only'],
    settings['restore_broken_parts_as_detached'],
    settings['allow_non_empty_tables'],
    mapContains(settings, 'password'),
    mapContains(settings, 'id')
from system.backups where id='$restore_id'
"

# The same must hold for the final (RESTORED) row persisted in system.backup_log.
${CLICKHOUSE_CLIENT} --query "system flush logs backup_log"
${CLICKHOUSE_CLIENT} -m --query "
select
    settings['structure_only'],
    settings['restore_broken_parts_as_detached'],
    settings['allow_non_empty_tables'],
    mapContains(settings, 'password'),
    mapContains(settings, 'id')
from system.backup_log where id='$restore_id' and status='RESTORED'
"

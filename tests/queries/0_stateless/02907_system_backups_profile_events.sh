#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
drop table if exists test;
create table test (a Int32) engine = MergeTree() order by tuple();
"

backup_id=${CLICKHOUSE_TEST_UNIQUE_NAME}
backup_name="Disk('backups', '$backup_id')";

${CLICKHOUSE_CLIENT} -m --query "
backup table ${CLICKHOUSE_DATABASE}.test to $backup_name;
" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} -m --query "
select ProfileEvents['BackupEntriesCollectorMicroseconds'] > 10 from system.backups where name='Disk(\'backups\', \'$backup_id\')'
"

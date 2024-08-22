#!/usr/bin/env bash
# Tags: no-ordinary-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
drop table if exists src;
create table src (a Int32) engine = MergeTree() order by tuple();

drop table if exists mv;
create materialized view mv (a Int32) engine = MergeTree() order by tuple() as select * from src;
"

uuid=$(${CLICKHOUSE_CLIENT} --query "select uuid from system.tables where table='mv' and database == currentDatabase()")
inner_table=".inner_id.${uuid}"
${CLICKHOUSE_CLIENT} -m --query "drop table \`$inner_table\` sync"

${CLICKHOUSE_CLIENT} -m --query "
set send_logs_level = 'error';
backup table ${CLICKHOUSE_DATABASE}.\`mv\` to Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}');
" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} -m --query "
drop table mv;
restore table ${CLICKHOUSE_DATABASE}.\`mv\` from Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}');
" | grep -o "RESTORED"

${CLICKHOUSE_CLIENT} --query "select count() from mv;"

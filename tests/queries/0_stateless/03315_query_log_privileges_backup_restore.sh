#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="u_03315_query_log_${CLICKHOUSE_DATABASE}"
table_name="d_03315_query_log"
backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
backup_query_prefix="backup table ${table_name} to "
backup_query="${backup_query_prefix}${backup_name}"
restore_query_prefix="restore all from "
restore_query="${restore_query_prefix}${backup_name}"

${CLICKHOUSE_CLIENT} --query "drop user if exists ${user_name}"
${CLICKHOUSE_CLIENT} --query "create user ${user_name}"
${CLICKHOUSE_CLIENT} --query "drop table if exists ${table_name}"
${CLICKHOUSE_CLIENT} --query "create table ${table_name} (a UInt64, b UInt64) order by a"
${CLICKHOUSE_CLIENT} --query "insert into table ${table_name} values (3315, 5133)"

${CLICKHOUSE_CLIENT} --query "grant current grants on *.* to ${user_name} with grant option"
${CLICKHOUSE_CLIENT} --query "revoke backup on *.* from ${user_name}"

${CLICKHOUSE_CLIENT} --user ${user_name} --query "${backup_query}" 2>&1 >/dev/null | (grep -q "ACCESS_DENIED" || echo "Expected ACCESS_DENIED error not found")

# Sometimes we might take the backup lock file content from the OS cache.
# That will lead to non-matching UUIDs of lock files (from the old failed backup and the new one), and a "concurrent backup is running" error will appear.
${CLICKHOUSE_CLIENT} --query "system drop mmap cache"

${CLICKHOUSE_CLIENT} --query "grant backup on *.* to ${user_name}"
${CLICKHOUSE_CLIENT} --user ${user_name} --query "${backup_query}" 2>&1 >/dev/null | (grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED error is not expected")

${CLICKHOUSE_CLIENT} --query "system flush logs query_log"
${CLICKHOUSE_CLIENT} --query "select used_privileges, missing_privileges from system.query_log where query ilike '${backup_query_prefix}%' and type = 'ExceptionBeforeStart' and current_database = currentDatabase() order by event_time desc limit 1"
${CLICKHOUSE_CLIENT} --query "select used_privileges, missing_privileges from system.query_log where query ilike '${backup_query_prefix}%' and type = 'QueryStart' and current_database = currentDatabase() order by event_time desc limit 1"
${CLICKHOUSE_CLIENT} --query "select used_privileges, missing_privileges from system.query_log where query ilike '${backup_query_prefix}%' and type = 'QueryFinish' and current_database = currentDatabase() order by event_time desc limit 1"

${CLICKHOUSE_CLIENT} --query "drop table ${table_name}"

${CLICKHOUSE_CLIENT} --query "revoke create, insert on *.* from ${user_name}"

${CLICKHOUSE_CLIENT} --user ${user_name} --query "${restore_query}" 2>&1 >/dev/null | (grep -q "ACCESS_DENIED" || echo "Expected ACCESS_DENIED error not found")

${CLICKHOUSE_CLIENT} --query "grant create, insert on *.* to ${user_name}"
${CLICKHOUSE_CLIENT} --user ${user_name} --query "${restore_query}" 2>&1 >/dev/null | (grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED error is not expected")

${CLICKHOUSE_CLIENT} --query "system flush logs query_log"
${CLICKHOUSE_CLIENT} --query "select used_privileges, missing_privileges from system.query_log where query ilike '${restore_query_prefix}%' and type = 'ExceptionBeforeStart' and current_database = currentDatabase() order by event_time desc limit 1"
${CLICKHOUSE_CLIENT} --query "select used_privileges, missing_privileges from system.query_log where query ilike '${restore_query_prefix}%' and type = 'QueryStart' and current_database = currentDatabase() order by event_time desc limit 1"
${CLICKHOUSE_CLIENT} --query "select used_privileges, missing_privileges from system.query_log where query ilike '${restore_query_prefix}%' and type = 'QueryFinish' and current_database = currentDatabase() order by event_time desc limit 1"

${CLICKHOUSE_CLIENT} --query "drop table ${table_name}"
${CLICKHOUSE_CLIENT} --query "drop user ${user_name}"

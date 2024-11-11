#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="u_03168_query_log"
table_name="default.d_03168_query_log"
test_query="select a, b from ${table_name}"

${CLICKHOUSE_CLIENT_BINARY} --query "drop user if exists ${user_name}"
${CLICKHOUSE_CLIENT_BINARY} --query "create user ${user_name}"
${CLICKHOUSE_CLIENT_BINARY} --query "drop table if exists ${table_name}"
${CLICKHOUSE_CLIENT_BINARY} --query "create table ${table_name} (a UInt64, b UInt64) order by a"

${CLICKHOUSE_CLIENT_BINARY} --query "insert into table ${table_name} values (3168, 8613)"

error="$(${CLICKHOUSE_CLIENT_BINARY} --user ${user_name} --query "${test_query}" 2>&1 >/dev/null)"
echo "${error}" | grep -Fc "ACCESS_DENIED"

${CLICKHOUSE_CLIENT_BINARY} --query "grant select(a, b) on ${table_name} to ${user_name}"

${CLICKHOUSE_CLIENT_BINARY} --user ${user_name} --query "${test_query}"

${CLICKHOUSE_CLIENT_BINARY} --query "system flush logs"
${CLICKHOUSE_CLIENT_BINARY} --query "select used_privileges, missing_privileges from system.query_log where query = '${test_query}' and type = 'ExceptionBeforeStart' and current_database = currentDatabase() order by event_time desc limit 1"
${CLICKHOUSE_CLIENT_BINARY} --query "select used_privileges, missing_privileges from system.query_log where query = '${test_query}' and type = 'QueryStart' and current_database = currentDatabase() order by event_time desc limit 1"
${CLICKHOUSE_CLIENT_BINARY} --query "select used_privileges, missing_privileges from system.query_log where query = '${test_query}' and type = 'QueryFinish' and current_database = currentDatabase() order by event_time desc limit 1"

${CLICKHOUSE_CLIENT_BINARY} --query "drop table ${table_name}"
${CLICKHOUSE_CLIENT_BINARY} --query "drop user ${user_name}"

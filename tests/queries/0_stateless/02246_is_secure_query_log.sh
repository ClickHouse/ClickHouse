#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --log_queries=1 --query_id "2246_${CLICKHOUSE_DATABASE}_client_nonsecure" -q "select 1 Format Null"
${CLICKHOUSE_CLIENT} -q "system flush logs"
${CLICKHOUSE_CLIENT} -q "select interface, is_secure from system.query_log where query_id = '2246_${CLICKHOUSE_DATABASE}_client_nonsecure' and type = 'QueryFinish' and current_database = currentDatabase()"

${CLICKHOUSE_CLIENT_SECURE} --log_queries=1 --query_id "2246_${CLICKHOUSE_DATABASE}_client_secure" -q "select 1 Format Null"
${CLICKHOUSE_CLIENT} -q "system flush logs"
${CLICKHOUSE_CLIENT} -q "select interface, is_secure from system.query_log where query_id = '2246_${CLICKHOUSE_DATABASE}_client_secure' and type = 'QueryFinish' and current_database = currentDatabase()"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&log_queries=1&query_id=2246_${CLICKHOUSE_DATABASE}_http_nonsecure" -d "select 1 Format Null"
${CLICKHOUSE_CLIENT} -q "system flush logs"
${CLICKHOUSE_CLIENT} -q "select interface, is_secure from system.query_log where query_id = '2246_${CLICKHOUSE_DATABASE}_http_nonsecure' and type = 'QueryFinish' and current_database = currentDatabase()"

${CLICKHOUSE_CURL} -sSk "${CLICKHOUSE_URL_HTTPS}&log_queries=1&query_id=2246_${CLICKHOUSE_DATABASE}_http_secure" -d "select 1 Format Null"
${CLICKHOUSE_CLIENT} -q "system flush logs"
${CLICKHOUSE_CLIENT} -q "select interface, is_secure from system.query_log where query_id = '2246_${CLICKHOUSE_DATABASE}_http_secure' and type = 'QueryFinish' and current_database = currentDatabase()"

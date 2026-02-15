#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --log_queries=1 --query_id "2246_${CLICKHOUSE_DATABASE}_client_nonsecure" -q "select 'native plain'"
${CLICKHOUSE_CLIENT_SECURE} --log_queries=1 --query_id "2246_${CLICKHOUSE_DATABASE}_client_secure" -q "select 'native secure'"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&log_queries=1&http_wait_end_of_query=1&query_id=2246_${CLICKHOUSE_DATABASE}_http_nonsecure" -d "select 'http plain'"
${CLICKHOUSE_CURL} -sSk "${CLICKHOUSE_URL_HTTPS}&log_queries=1&http_wait_end_of_query=1&query_id=2246_${CLICKHOUSE_DATABASE}_http_secure" -d "select 'http secure'"

# Wait for all 4 queries (2 native + 2 HTTP) to appear in query_log.
# There is a race between HTTP response being sent and the query_log entry being written.
for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} -q "system flush logs query_log"
    count=$(${CLICKHOUSE_CLIENT} -q "select count() from system.query_log where query_id LIKE '2246_${CLICKHOUSE_DATABASE}_%' and type = 'QueryFinish' and current_database = currentDatabase()")
    [ "$count" -ge 4 ] && break
    sleep 0.5
done

${CLICKHOUSE_CLIENT} -q "select interface, is_secure from system.query_log where query_id = '2246_${CLICKHOUSE_DATABASE}_client_nonsecure' and type = 'QueryFinish' and current_database = currentDatabase()"
${CLICKHOUSE_CLIENT} -q "select interface, is_secure from system.query_log where query_id = '2246_${CLICKHOUSE_DATABASE}_client_secure' and type = 'QueryFinish' and current_database = currentDatabase()"
${CLICKHOUSE_CLIENT} -q "select interface, is_secure from system.query_log where query_id = '2246_${CLICKHOUSE_DATABASE}_http_nonsecure' and type = 'QueryFinish' and current_database = currentDatabase()"
${CLICKHOUSE_CLIENT} -q "select interface, is_secure from system.query_log where query_id = '2246_${CLICKHOUSE_DATABASE}_http_secure' and type = 'QueryFinish' and current_database = currentDatabase()"

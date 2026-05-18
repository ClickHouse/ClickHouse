#!/usr/bin/env bash
set -ue

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

${CLICKHOUSE_CLIENT} -q "select 1 format Null" "--query_id=$query_id"

${CLICKHOUSE_CURL} \
    --header "X-ClickHouse-Query-Id: $query_id" \
    $CLICKHOUSE_URL \
    --get \
    --data-urlencode "query=select 1 format Null"

# Wait for both TCP and HTTP queries to appear in query_log.
# There is a race between HTTP response being sent and the query_log entry being written.
for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} -q "system flush logs query_log"
    count=$(${CLICKHOUSE_CLIENT} -q "select count() from system.query_log where current_database = currentDatabase() AND query_id = '$query_id' and type = 'QueryFinish'")
    [ "$count" -ge 2 ] && break
    sleep 0.5
done

${CLICKHOUSE_CLIENT} -q "
select interface, initial_query_id = query_id
    from system.query_log
    where current_database = currentDatabase() AND query_id = '$query_id' and type = 'QueryFinish'
    order by interface
    ;
"


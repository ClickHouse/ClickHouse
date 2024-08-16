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

${CLICKHOUSE_CLIENT} -q "
system flush logs;
select interface, initial_query_id = query_id
    from system.query_log
    where current_database = currentDatabase() AND query_id = '$query_id' and type = 'QueryFinish'
    order by interface
    ;
"


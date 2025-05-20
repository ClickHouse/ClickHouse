#!/usr/bin/env bash
set -ue

# this test doesn't need 'current_database = currentDatabase()',

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "drop table if exists m"
${CLICKHOUSE_CLIENT} -q "create table m (dummy UInt8) ENGINE = Distributed('test_cluster_two_shards', 'system', 'one')"

query_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} -q "select * from m format Null" "--query_id=$query_id"

${CLICKHOUSE_CLIENT} -q "
system flush logs;
select
    anyIf(initial_query_start_time, is_initial_query) = anyIf(initial_query_start_time, not is_initial_query),
    anyIf(initial_query_start_time_microseconds, is_initial_query) = anyIf(initial_query_start_time_microseconds, not is_initial_query)
from system.query_log
where initial_query_id = '$query_id' and type = 'QueryFinish';
"

${CLICKHOUSE_CLIENT} -q "drop table m"

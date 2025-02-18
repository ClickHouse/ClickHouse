#!/usr/bin/env bash
# Tags: long, no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="group-by-mem-usage-$CLICKHOUSE_DATABASE"

echo "Spin up a long running query"
${CLICKHOUSE_CLIENT} --query "with q as (select length(groupArray(toString(number))) as x  from numbers_mt(2e6) group by number order by x limit 1), q1 as (select * from q), q2 as (select * from q), q3 as (select * from q), q4 as (select * from q) select * from q, q1, q2, q3, q4 settings max_bytes_before_external_group_by='1G', max_memory_usage='2G'" --query_id "$query_id"
${CLICKHOUSE_CLIENT} --query "system flush logs"
${CLICKHOUSE_CLIENT} --query "select ProfileEvents['ExternalAggregationWritePart'] from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = '$query_id' and event_date >= today() - 1"

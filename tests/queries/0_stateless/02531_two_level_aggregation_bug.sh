#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# It is totally ok if sometimes some of the query processing threads did not process any data, as all the data processed by the other threads.
# Check that at least once all 6 threads converted their aggregation data into two-level hash table.

while true
do
    query_id=$(echo "select queryID() from (select sum(s), k from remote('127.0.0.{1,2}', view(select sum(number) s, bitAnd(number, 3) k from numbers_mt(1000000) group by k)) group by k) limit 1 settings group_by_two_level_threshold=1, max_threads=3, prefer_localhost_replica=1" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @- 2>&1)

    ${CLICKHOUSE_CLIENT} --query="system flush logs"
    ${CLICKHOUSE_CLIENT} --query="select count() from system.text_log where event_date >= yesterday() and query_id = '${query_id}' and message like '%Converting aggregation data to two-level%' SETTINGS max_rows_to_read = 0" | grep -P '^6$' && break;
done

#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TAG="$RANDOM$RANDOM$RANDOM"

$CLICKHOUSE_CURL -v -sS "$CLICKHOUSE_URL&query_id=${CLICKHOUSE_DATABASE}_q1&use_query_cache=1&query_cache_ttl=600&query_cache_tag=${TAG}&query=SELECT+'Test+03711'" |& grep -o -P '< (Age|Expires):'
sleep 1
$CLICKHOUSE_CURL -v -sS "$CLICKHOUSE_URL&query_id=${CLICKHOUSE_DATABASE}_q2&use_query_cache=1&query_cache_ttl=600&query_cache_tag=${TAG}&query=SELECT+'Test+03711'" |& grep -o -P '< (Age|Expires):'

$CLICKHOUSE_CLIENT --query "
SYSTEM FLUSH LOGS query_log;

SELECT replace(query_id, currentDatabase(), ''), ProfileEvents['QueryCacheAgeSeconds'] >= 1,
    ProfileEvents['QueryCacheReadRows'], ProfileEvents['QueryCacheReadBytes'] > 10,
    ProfileEvents['QueryCacheWrittenRows'], ProfileEvents['QueryCacheWrittenBytes'] > 10
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND query_id IN ('${CLICKHOUSE_DATABASE}_q1', '${CLICKHOUSE_DATABASE}_q2')
    AND Settings['query_cache_tag'] = '$TAG'
ORDER BY query_id;
"

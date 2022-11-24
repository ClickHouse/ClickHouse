#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id=$RANDOM
${CLICKHOUSE_CLIENT} --query_id $query_id --query "SELECT 1 FORMAT Null SETTINGS trace_profile_events = 0"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} --query "SELECT count() = 0 FROM system.trace_log WHERE query_id = '$query_id' AND trace_type = 'ProfileEvent'"

query_id=$RANDOM
${CLICKHOUSE_CLIENT} --query_id $query_id --query "SELECT 1 FORMAT Null SETTINGS trace_profile_events = 1"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0, sum(empty(trace)) = 0 FROM system.trace_log WHERE query_id = '$query_id' AND trace_type = 'ProfileEvent'"

query_id=$RANDOM
${CLICKHOUSE_CLIENT} --query_id $query_id --query "SELECT count() FROM numbers_mt(1000000) FORMAT Null SETTINGS trace_profile_events = 1"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} --query "
SELECT count()
FROM
(
    (
        SELECT
            Events.1 AS event,
            Events.2 AS value
        FROM system.query_log
        ARRAY JOIN CAST(ProfileEvents, 'Array(Tuple(String, Int64))') AS Events
        WHERE query_id = '$query_id' AND type = 'QueryFinish'
        ORDER BY event
    )
    EXCEPT
    (
        SELECT
            event,
            sum(increment) AS value
        FROM system.trace_log
        WHERE (trace_type = 'ProfileEvent') AND (query_id = '$query_id') AND increment != 0
        GROUP BY event
        ORDER BY event ASC
    )
)
WHERE event NOT IN ('ContextLock', 'NetworkSendBytes', 'NetworkSendElapsedMicroseconds');
"

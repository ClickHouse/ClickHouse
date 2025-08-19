#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs pv
# Tag no-parallel: reads from a system table

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t; CREATE TABLE t (x UInt64) ENGINE = Memory;"

# Rate limit is chosen for operation to spent more than one second.
seq 1 1000 | pv --quiet --rate-limit 400 | ${CLICKHOUSE_CLIENT} --query "INSERT INTO t FORMAT TSV"

# We check that the value of NetworkReceiveElapsedMicroseconds correctly includes the time spent waiting data from the client.
result=$(${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS;
    WITH ProfileEvents['NetworkReceiveElapsedMicroseconds'] AS elapsed_us
    SELECT elapsed_us FROM system.query_log
    WHERE current_database = currentDatabase() AND query_kind = 'Insert' AND event_date >= yesterday() AND type = 'QueryFinish'
    ORDER BY event_time DESC LIMIT 1;")

elapsed_us=$(echo $result | sed 's/ .*//')

min_elapsed_us=1000000
if [[ "$elapsed_us" -ge "$min_elapsed_us" ]]; then
    echo 1
else
    # Print debug info
    ${CLICKHOUSE_CLIENT} --query "
    WITH ProfileEvents['NetworkReceiveElapsedMicroseconds'] AS elapsed_us
    SELECT query_start_time_microseconds, event_time_microseconds, query_duration_ms, elapsed_us, query FROM system.query_log
    WHERE current_database = currentDatabase() and event_date >= yesterday() AND type = 'QueryFinish' ORDER BY query_start_time;"
fi

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"

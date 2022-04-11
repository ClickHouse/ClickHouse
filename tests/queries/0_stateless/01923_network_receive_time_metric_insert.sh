#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs pv

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery --query "DROP TABLE IF EXISTS t; CREATE TABLE t (x UInt64) ENGINE = Memory;"

# Rate limit is chosen for operation to spent more than one second.
seq 1 1000 | pv --quiet --rate-limit 1000 | ${CLICKHOUSE_CLIENT} --query "INSERT INTO t FORMAT TSV"

# We check that the value of NetworkReceiveElapsedMicroseconds correctly includes the time spent waiting data from the client.
${CLICKHOUSE_CLIENT} --multiquery --query "SYSTEM FLUSH LOGS;
    WITH ProfileEvents['NetworkReceiveElapsedMicroseconds'] AS time
    SELECT time >= 1000000 ? 1 : time FROM system.query_log
        WHERE current_database = currentDatabase() AND query_kind = 'Insert' AND event_date >= yesterday() AND type = 2 ORDER BY event_time DESC LIMIT 1;"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"

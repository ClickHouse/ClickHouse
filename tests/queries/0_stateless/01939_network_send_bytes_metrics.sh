#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery --query "DROP TABLE IF EXISTS t; CREATE TABLE t (x UInt64) ENGINE = Memory;"

${CLICKHOUSE_CLIENT} --query "SELECT number FROM numbers(1000)" > /dev/null

${CLICKHOUSE_CLIENT} --multiquery --query "SYSTEM FLUSH LOGS;
    WITH ProfileEvents['NetworkSendBytes'] AS bytes
    SELECT bytes >= 8000 AND bytes < 9000 ? 1 : bytes FROM system.query_log
        WHERE current_database = currentDatabase() AND query_kind = 'Select' AND event_date >= yesterday() AND type = 2 ORDER BY event_time DESC LIMIT 1;"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"

#!/usr/bin/env bash
# Tags: no-tsan, no-parallel
# Tag no-parallel: waits for `SYSTEM FLUSH LOGS trace_log` twice, which under parallel load lags for minutes: the flush thread symbolizes every sampled stack (126-150 s per flush in debug builds), so the test exceeds its time limit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="$RANDOM-$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CLIENT} --query_id $query_id --query "SELECT 1 FORMAT Null SETTINGS trace_profile_events = 0"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS trace_log"
${CLICKHOUSE_CLIENT} --query "SELECT count() = 0 FROM system.trace_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '$query_id' AND trace_type = 'ProfileEvent'"

query_id="$RANDOM-$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CLIENT} --query_id $query_id --query "SELECT 1 FORMAT Null SETTINGS trace_profile_events = 1"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS trace_log"
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0, sum(empty(trace)) = 0 FROM system.trace_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '$query_id' AND trace_type = 'ProfileEvent'"

#!/usr/bin/env bash
# Tags: no-tsan, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="$RANDOM-$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CLIENT} --query_id $query_id --query "SELECT 1 FORMAT Null SETTINGS trace_profile_events = 0"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS trace_log"
${CLICKHOUSE_CLIENT} --query "SELECT count() = 0 FROM system.trace_log WHERE query_id = '$query_id' AND trace_type = 'ProfileEvent'"

query_id="$RANDOM-$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CLIENT} --query_id $query_id --query "SELECT 1 FORMAT Null SETTINGS trace_profile_events = 1"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS trace_log"
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0, sum(empty(trace)) = 0 FROM system.trace_log WHERE query_id = '$query_id' AND trace_type = 'ProfileEvent'"

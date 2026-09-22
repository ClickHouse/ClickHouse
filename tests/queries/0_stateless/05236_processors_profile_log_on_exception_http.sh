#!/usr/bin/env bash

# `processors_profile_log` must have entries for a query that fails during execution over HTTP.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_DATABASE}_processors_profile_log_on_exception_http"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${QUERY_ID}&log_processors_profiles=1" \
    -d "SELECT throwIf(number = 3) FROM numbers(10) FORMAT Null" | grep -o 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO' | head -1

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS processors_profile_log"

${CLICKHOUSE_CLIENT} -q "
    SELECT count() > 0, countIf(output_rows > 0) > 0, countIf(exception_code = 395) = count()
    FROM system.processors_profile_log
    WHERE event_date >= yesterday() AND query_id = '${QUERY_ID}'"

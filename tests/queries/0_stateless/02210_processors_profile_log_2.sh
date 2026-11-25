#!/usr/bin/env bash

set -eo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_ID=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")


${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID}" <<EOF
SELECT sum(number) FROM numbers_mt(1000000)
SETTINGS max_threads = 4, log_processors_profiles=true, log_queries=1, log_queries_min_type='QUERY_FINISH';
EOF

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS processors_profile_log"

${CLICKHOUSE_CLIENT} -q "select name, sum(input_rows), sum(input_bytes), sum(output_rows), sum(output_bytes) from system.processors_profile_log where event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '${QUERY_ID}' group by name, plan_step, plan_group order by name, sum(input_rows), sum(input_bytes), sum(output_rows), sum(output_bytes)"

${CLICKHOUSE_CLIENT} -q "select countDistinct(initial_query_id) from system.processors_profile_log where event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '${QUERY_ID}'"

# The pipeline dump, with its processor nodes, is written to the test-level log of a finished query and of a failed one, and only with log_processors_profiles.
function dump_in_log()
{
    (${CLICKHOUSE_CLIENT} --send_logs_level=test "$@" 2>&1 || :) | awk '/Processors profile log:/ { m++ } /Input wait time:/ { n++ } END { print m + 0, (n > 0 ? 1 : 0) }'
}
dump_in_log --log_processors_profiles=1 -q "SELECT sum(number) FROM numbers_mt(1000) SETTINGS max_threads = 2 FORMAT Null"
dump_in_log --log_processors_profiles=1 -q "SELECT throwIf(number = 3) FROM numbers(10) FORMAT Null"
dump_in_log --log_processors_profiles=0 -q "SELECT 1 FORMAT Null"

#!/usr/bin/env bash

set -eo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_ID=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")


${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID}" <<EOF
SELECT sum(number) FROM numbers_mt(1000000)
SETTINGS log_processors_profiles=true, log_queries=1, log_queries_min_type='QUERY_FINISH';
EOF

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"

${CLICKHOUSE_CLIENT} -q "select any(name) name, sum(input_rows), sum(input_bytes), sum(output_rows), sum(output_bytes) from system.processors_profile_log where query_id = '${QUERY_ID}' group by plan_step, plan_group order by name, sum(input_rows), sum(input_bytes), sum(output_rows), sum(output_bytes)"

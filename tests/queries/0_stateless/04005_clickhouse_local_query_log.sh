#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04005_clickhouse_local_query_log"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

CONFIG_FILE="${WORKING_FOLDER}/config.xml"
cat > "${CONFIG_FILE}" << 'EOF'
<clickhouse>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
    </query_log>
</clickhouse>
EOF

# With a config file defining query_log, system.query_log should be available
${CLICKHOUSE_LOCAL} \
    --path "${WORKING_FOLDER}" \
    --config-file "${CONFIG_FILE}" \
    --log_queries=1 \
    --query "SELECT 42 AS answer; SYSTEM FLUSH LOGS; SELECT count() > 0 FROM system.query_log WHERE query LIKE '%SELECT 42%' AND current_database = currentDatabase()"

# With --no-system-tables, system.query_log should not be available even with a config
${CLICKHOUSE_LOCAL} \
    --path "${WORKING_FOLDER}" \
    --config-file "${CONFIG_FILE}" \
    --no-system-tables \
    --log_queries=1 \
    --query "SELECT count() FROM system.query_log" 2>&1 | grep -o "UNKNOWN_TABLE"

rm -rf "${WORKING_FOLDER}"

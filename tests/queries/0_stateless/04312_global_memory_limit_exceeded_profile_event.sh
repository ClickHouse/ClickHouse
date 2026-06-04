#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --ignore-error --multiquery -- --max_server_memory_usage=0 2>/dev/null <<'EOF'
SELECT number
FROM system.numbers
GROUP BY number
SETTINGS max_memory_usage = '16Mi', max_bytes_ratio_before_external_group_by = 0
FORMAT Null;

SELECT 'query limit leaves global event unchanged', value = 0
FROM system.events
WHERE event = 'GlobalMemoryLimitExceeded'
SETTINGS system_events_show_zero_values = 1;
EOF

${CLICKHOUSE_LOCAL} --ignore-error --multiquery -- --max_server_memory_usage=100Mi 2>/dev/null <<'EOF'
SELECT number
FROM system.numbers
GROUP BY number
SETTINGS max_memory_usage = 0, max_bytes_ratio_before_external_group_by = 0
FORMAT Null;

SELECT 'global limit increments global event', value > 0
FROM system.events
WHERE event = 'GlobalMemoryLimitExceeded'
SETTINGS system_events_show_zero_values = 1;
EOF

#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

trap '${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT time_series_inner_table_flush_sleep"' EXIT

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT time_series_inner_table_flush_sleep"

${CLICKHOUSE_CLIENT} --multiquery <<'EOF'
SET allow_experimental_time_series_table = 1;
DROP TABLE IF EXISTS ts_speed;
CREATE TABLE ts_speed ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;
EOF

insert_with_comment()
{
    local comment="$1"
    local threads="$2"
    local env_value="$3"
    local sample_value="$4"
    local client="${CLICKHOUSE_CLIENT}"
    if [[ -v CLICKHOUSE_LOG_COMMENT ]]; then
        client="${CLICKHOUSE_CLIENT/--log_comment ${CLICKHOUSE_LOG_COMMENT}/--log_comment ${comment}}"
    else
        client="${CLICKHOUSE_CLIENT} --log_comment ${comment}"
    fi
    ${client} --query "INSERT INTO ts_speed (metric_name, tags, samples) SETTINGS max_threads = ${threads}, log_comment = '${comment}' VALUES ('m', map('env', '${env_value}'), [(now64(3) - INTERVAL 1 MINUTE, ${sample_value})])"
}

insert_with_comment ts_flush_serial 1 prod 1.
insert_with_comment ts_flush_parallel 8 dev 2.

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT time_series_inner_table_flush_sleep"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

serial=$(${CLICKHOUSE_CLIENT} --query "SELECT max(query_duration_ms) FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = 'ts_flush_serial' AND type = 'QueryFinish' AND query LIKE '%INSERT INTO ts_speed%'")
parallel=$(${CLICKHOUSE_CLIENT} --query "SELECT max(query_duration_ms) FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = 'ts_flush_parallel' AND type = 'QueryFinish' AND query LIKE '%INSERT INTO ts_speed%'")

${CLICKHOUSE_CLIENT} --query "DROP TABLE ts_speed"

if [[ -z "${serial}" || "${serial}" -lt 2500 ]]; then
    echo "Serial insert took ${serial:-missing} ms" >&2
    exit 1
fi

if [[ -z "${parallel}" || $((parallel * 3)) -ge $((serial * 2)) ]]; then
    echo "Parallel insert took ${parallel:-missing} ms and serial insert took ${serial} ms" >&2
    exit 1
fi

echo OK

#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-async-insert
# The tags table commits first. The samples and recent samples tables are written in parallel after it.
# The CHECK constraints sleep: 1 s on tags, 3 s on samples and 3 s on recent samples.
# A serial insert takes about 7 s. A parallel insert takes about 4 s, so it saves at least 1.5 s.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

${CLICKHOUSE_CLIENT} --multiquery <<'EOF'
SET allow_experimental_time_series_table = 1;
DROP TABLE IF EXISTS ts_speed;

CREATE TABLE ts_speed UUID '05297000-0000-4000-8000-000000000001' ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;

ALTER TABLE `.inner_id.tags.05297000-0000-4000-8000-000000000001` ADD CONSTRAINT slow CHECK sleepEachRow(1) = 0;
ALTER TABLE `.inner_id.samples.05297000-0000-4000-8000-000000000001` ADD CONSTRAINT slow CHECK sleepEachRow(3) = 0;
ALTER TABLE `.inner_id.recentsamples.05297000-0000-4000-8000-000000000001` ADD CONSTRAINT slow CHECK sleepEachRow(3) = 0;
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

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

serial=$(${CLICKHOUSE_CLIENT} --query "SELECT max(query_duration_ms) FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = 'ts_flush_serial' AND type = 'QueryFinish' AND query LIKE '%INSERT INTO ts_speed%'")
parallel=$(${CLICKHOUSE_CLIENT} --query "SELECT max(query_duration_ms) FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = 'ts_flush_parallel' AND type = 'QueryFinish' AND query LIKE '%INSERT INTO ts_speed%'")

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM \`.inner_id.tags.05297000-0000-4000-8000-000000000001\`"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM \`.inner_id.samples.05297000-0000-4000-8000-000000000001\`"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM \`.inner_id.recentsamples.05297000-0000-4000-8000-000000000001\`"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ts_speed"

if [[ -z "${serial}" || "${serial}" -lt 6000 ]]; then
    echo "Serial insert took ${serial:-missing} ms" >&2
    exit 1
fi

if [[ -z "${parallel}" || $((serial - parallel)) -lt 1500 ]]; then
    echo "Parallel insert took ${parallel:-missing} ms and serial insert took ${serial} ms" >&2
    exit 1
fi

echo OK

#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-replicated-database: `DatabaseReplicated` does not drop `TimeSeries` inner tables synchronously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ts_recent_fault ENGINE = TimeSeries
    SETTINGS recent_samples_ttl_seconds = 3600, recent_samples_partition_by = 'toStartOfHour(timestamp)'"

SAMPLES=$(${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.tables
    WHERE database = currentDatabase() AND name LIKE '.inner_id.samples.%'")
RECENT=$(${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.tables
    WHERE database = currentDatabase() AND name LIKE '.inner_id.recentsamples.%'")

${CLICKHOUSE_CLIENT} -q "ALTER TABLE \`${SAMPLES}\` ADD COLUMN _fail UInt8 MATERIALIZED throwIf(1, 'samples write failed')"

if ${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 --send_logs_level=fatal -q "
    INSERT INTO ts_recent_fault (metric_name, tags, time_series) VALUES
    ('fault_metric', map(), [(toDateTime64('2000-01-01 10:00:00', 3), 1.)])" >/dev/null 2>&1
then
    echo 'unexpected success'
else
    echo 'insert failed'
fi

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM \`${SAMPLES}\`"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM \`${RECENT}\`"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ts_recent_fault SYNC"

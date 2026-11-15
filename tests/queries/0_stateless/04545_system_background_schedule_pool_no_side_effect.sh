#!/usr/bin/env bash
# Reading system.background_schedule_pool must not create a schedule pool as a side effect.
# clickhouse-local always creates the general pool for the DDL background tasks; the other five are
# created lazily, so their *SchedulePoolSize metrics must still be 0 after the table is read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

local_path="${CLICKHOUSE_TMP:?}/04545_local_$$"
rm -rf "$local_path"

${CLICKHOUSE_LOCAL} --path="$local_path" --only-system-tables --multiquery "
SELECT 'before', metric, value FROM system.metrics WHERE metric LIKE '%SchedulePoolSize' AND metric != 'BackgroundSchedulePoolSize' ORDER BY metric;
SELECT count() >= 0 FROM system.background_schedule_pool FORMAT Null;
SELECT 'after', metric, value FROM system.metrics WHERE metric LIKE '%SchedulePoolSize' AND metric != 'BackgroundSchedulePoolSize' ORDER BY metric;
"

rm -rf "$local_path"

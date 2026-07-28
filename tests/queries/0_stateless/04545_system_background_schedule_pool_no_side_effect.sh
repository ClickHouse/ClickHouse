#!/usr/bin/env bash
# Reading system.background_schedule_pool must not create a schedule pool as a side effect.
# All six pools are created lazily, so in clickhouse-local none of them exists yet: every
# *SchedulePoolSize metric must still be 0 after the table is read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --multiquery "
SELECT 'before', metric, value FROM system.metrics WHERE metric LIKE '%SchedulePoolSize' ORDER BY metric;
SELECT count() >= 0 FROM system.background_schedule_pool FORMAT Null;
SELECT 'after', metric, value FROM system.metrics WHERE metric LIKE '%SchedulePoolSize' ORDER BY metric;
"

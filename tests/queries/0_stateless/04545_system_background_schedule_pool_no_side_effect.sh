#!/usr/bin/env bash
# Reading system.background_schedule_pool must not create a schedule pool as a side effect.
# The schedule pools are created lazily; the iceberg pool in particular is created only by an
# active Iceberg table. Use clickhouse-local (no Iceberg tables) and check the
# IcebergSchedulePoolSize metric stays 0 after querying the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --multiquery "
SELECT 'iceberg_pool_before', value FROM system.metrics WHERE metric = 'IcebergSchedulePoolSize';
SELECT count() >= 0 FROM system.background_schedule_pool FORMAT Null;
SELECT 'iceberg_pool_after', value FROM system.metrics WHERE metric = 'IcebergSchedulePoolSize';
"

#!/usr/bin/env bash
# Tags: no-fasttest
# Parquet TIME is time-of-day in [00:00:00, 24:00:00). ClickHouse Time/Time64 allow a wider
# domain (e.g. negative values and 100:00:00); reject those on write.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Valid values must still write successfully.
${CLICKHOUSE_CLIENT} -q "SELECT toTime('23:59:59') FORMAT Parquet" > /dev/null && echo "Time OK"
${CLICKHOUSE_CLIENT} -q "SELECT toTime64('23:59:59.999999', 6) FORMAT Parquet" > /dev/null && echo "Time64(6) OK"
${CLICKHOUSE_CLIENT} -q "SELECT toTime64('23:59:59.999999999', 9) FORMAT Parquet" > /dev/null && echo "Time64(9) OK"

# Out of range: beyond 24 hours.
${CLICKHOUSE_CLIENT} -q "SELECT toTime('100:00:00') FORMAT Parquet" 2>&1 | grep -o 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'
${CLICKHOUSE_CLIENT} -q "SELECT toTime64('100:00:00', 6) FORMAT Parquet" 2>&1 | grep -o 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'
${CLICKHOUSE_CLIENT} -q "SELECT toTime64('100:00:00', 3) FORMAT Parquet" 2>&1 | grep -o 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'

# Out of range: negative.
${CLICKHOUSE_CLIENT} -q "SELECT toTime('-01:00:00') FORMAT Parquet" 2>&1 | grep -o 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'
${CLICKHOUSE_CLIENT} -q "SELECT toTime64('-01:00:00', 6) FORMAT Parquet" 2>&1 | grep -o 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'

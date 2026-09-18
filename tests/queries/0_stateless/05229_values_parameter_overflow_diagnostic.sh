#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE dt_overflow (v DateTime('UTC')) ENGINE = Memory"

# A parameter is baked into the expression template as a constant, so the overflow is caught while the
# template is built. The fallback's TYPE_MISMATCH must not hide it.
$CLICKHOUSE_CLIENT --date_time_overflow_behavior='throw' --param_value='1900-01-01 00:00:00' \
    -q "INSERT INTO dt_overflow VALUES ({value:DateTime64(9)})" 2>&1 |
    grep -c -m1 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'

$CLICKHOUSE_CLIENT --date_time_overflow_behavior='saturate' --param_value='1900-01-01 00:00:00' \
    -q "INSERT INTO dt_overflow VALUES ({value:DateTime64(9)})"
$CLICKHOUSE_CLIENT -q "SELECT v FROM dt_overflow"

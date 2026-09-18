#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE dt_overflow (v DateTime('UTC')) ENGINE = Memory"

# A query parameter is substituted into the expression template as a constant instead of a placeholder,
# so the overflow is hit while the template is built and the error is caught. The single-expression
# fallback cannot convert DateTime64 to DateTime at all, and its TYPE_MISMATCH must not hide the real one.
$CLICKHOUSE_CLIENT --date_time_overflow_behavior='throw' --param_value='1900-01-01 00:00:00' \
    -q "INSERT INTO dt_overflow VALUES ({value:DateTime64(9)})" 2>&1 |
    grep -c -m1 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'

$CLICKHOUSE_CLIENT --date_time_overflow_behavior='saturate' --param_value='1900-01-01 00:00:00' \
    -q "INSERT INTO dt_overflow VALUES ({value:DateTime64(9)})"
$CLICKHOUSE_CLIENT -q "SELECT v FROM dt_overflow"

#!/usr/bin/env bash
# Companion to 04401_date_time_overflow_behavior_numeric_casts.sql, which asserts only the error
# code. The rendered message is the only place the throw path's float widening is observable:
# narrowing a huge, infinite or NaN float to Int64 instead would be undefined behavior.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for target in Date Date32 DateTime Time; do
    for value in "1e300::Float64" "3e38::Float32" "inf::Float64" "-inf::Float64" "nan::Float64"; do
        # A non-finite float is unconvertible rather than out of range, so accept that message too:
        # matching only the out-of-bounds form leaves those rows empty, which would pass whatever the
        # server printed.
        message=$(${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type=1 --date_time_overflow_behavior=throw \
            --query "SELECT CAST($value, '$target')" 2>&1 \
            | grep -oE "(Timestamp value|Value) [^ ]+ is out of bounds of type $target|Unexpected inf or nan to integer conversion" | head -1)
        echo "$value -> $target: ${message:-NO MATCH}"
    done
done

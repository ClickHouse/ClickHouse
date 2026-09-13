#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

env TZ=Asia/Kolkata ${CLICKHOUSE_CLIENT} --use_client_time_zone=1 --session_timezone=UTC --date_time_output_format=simple --multiquery --query "
SELECT 1 FORMAT Null;
CREATE TEMPORARY TABLE client_lazy_timezone (dt DateTime, dt64 DateTime64(3)) ENGINE = Memory;
INSERT INTO client_lazy_timezone SELECT toDateTime(86400), fromUnixTimestamp64Milli(toInt64(86400125));
SET session_timezone = DEFAULT;
SELECT timezone(), dt, dt64 FROM client_lazy_timezone;
SET session_timezone = 'America/New_York';
SELECT timezone(), toDateTime(1593561600), fromUnixTimestamp64Milli(toInt64(1593561600125));
SET session_timezone = 'UTC';
SELECT timezone(), dt, dt64 FROM client_lazy_timezone;
SET session_timezone = DEFAULT;
SELECT timezone(), dt, dt64 FROM client_lazy_timezone;
SELECT toDateTime(86400, 'UTC'), toDateTime(86400, 'Asia/Kolkata'), fromUnixTimestamp64Milli(toInt64(86400125), 'UTC'), fromUnixTimestamp64Milli(toInt64(86400125), 'Asia/Kolkata');
SELECT toTypeName(toDateTime(86400, 'Asia/Kolkata')), toTypeName(fromUnixTimestamp64Milli(toInt64(86400125), 'Asia/Kolkata'));
DROP TABLE client_lazy_timezone;
"

for data_type in "DateTime('Invalid/LazyTimezone')" "DateTime64(3, 'Invalid/LazyTimezone')"; do
    if output=$(${CLICKHOUSE_CLIENT} --query "CREATE TEMPORARY TABLE invalid_lazy_timezone (value ${data_type}) ENGINE = Memory" 2>&1); then
        echo "Invalid timezone was accepted for ${data_type}"
        exit 1
    fi
    if [[ "$output" != *"Cannot load time zone Invalid/LazyTimezone"* ]]; then
        printf '%s\n' "$output"
        exit 1
    fi
    echo "Invalid timezone rejected"
done

for binary_types in 0 1; do
    ${CLICKHOUSE_CLIENT} --output_format_native_encode_types_in_binary_format="$binary_types" \
        --input_format_native_decode_types_in_binary_format="$binary_types" --query "
        SELECT
            toDateTime(number * 2147483647, 'Asia/Kolkata') AS dt,
            CAST(toInt64(number) - 1 AS DateTime64(0, 'UTC')) AS dt64_seconds,
            fromUnixTimestamp64Milli(toInt64(number) * 1000 - 1001, 'America/New_York') AS dt64_millis,
            fromUnixTimestamp64Nano(toInt64(number) - 1, 'UTC') AS dt64_nanos
        FROM numbers(3)
        FORMAT Native
    " | ${CLICKHOUSE_LOCAL} --input-format Native --output-format TSV --input_format_native_decode_types_in_binary_format="$binary_types" --query "
        SELECT
            toUInt32(dt),
            toInt64(dt64_seconds),
            toUnixTimestamp64Milli(dt64_millis),
            toUnixTimestamp64Nano(dt64_nanos),
            toTypeName(dt),
            toTypeName(dt64_seconds),
            toTypeName(dt64_millis),
            toTypeName(dt64_nanos)
        FROM table
        ORDER BY dt
    "
done

for binary_types in 0 1; do
    ${CLICKHOUSE_CLIENT} --output_format_native_encode_types_in_binary_format="$binary_types" \
        --input_format_native_decode_types_in_binary_format="$binary_types" --query "
        SELECT
            number,
            arrayResize([toDateTime(number * 2147483647, 'Asia/Kolkata')], number) AS array_dt,
            if(number = 1, NULL, fromUnixTimestamp64Milli(toInt64(number) * 1000 - 1001, 'America/New_York')) AS nullable_dt64,
            tuple(toDateTime(number * 2147483647, 'Asia/Kolkata'), fromUnixTimestamp64Nano(toInt64(number) - 1, 'UTC')) AS tuple_dt,
            toLowCardinality(toDateTime(number * 2147483647, 'Asia/Kolkata')) AS low_cardinality_dt
        FROM numbers(3)
        FORMAT Native
    " | ${CLICKHOUSE_LOCAL} --input-format Native --output-format TSV --input_format_native_decode_types_in_binary_format="$binary_types" --query "
        SELECT
            count(),
            countIf(arrayMap(value -> toUInt32(value), array_dt) = arrayResize([number * 2147483647], number)),
            countIf(if(number = 1, isNull(nullable_dt64), toUnixTimestamp64Milli(nullable_dt64) = toInt64(number) * 1000 - 1001)),
            countIf(toUInt32(tuple_dt.1) = number * 2147483647 AND toUnixTimestamp64Nano(tuple_dt.2) = toInt64(number) - 1),
            countIf(toUInt32(low_cardinality_dt) = number * 2147483647),
            any(toTypeName(array_dt)),
            any(toTypeName(nullable_dt64)),
            any(toTypeName(tuple_dt)),
            any(toTypeName(low_cardinality_dt))
        FROM table
    "
done

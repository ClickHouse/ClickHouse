#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

expect_error()
{
    local expected="$1"
    local query="$2"
    local output
    if output=$($CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 -q "$query" 2>&1); then
        echo "Expected $expected, but the query succeeded" >&2
        exit 1
    fi
    if [[ "$output" != *"$expected"* ]]; then
        echo "Expected $expected, got: $output" >&2
        exit 1
    fi
    echo "$expected"
}

# Failed full-definition ATTACH can leave a UUID mapping until cleanup. A fresh UUID
# per invocation prevents the flaky-check rerun from colliding with the first run.
attach_raw_uuid=$($CLICKHOUSE_CLIENT -q 'SELECT generateUUIDv4()')
attach_inner_uuid=$($CLICKHOUSE_CLIENT -q 'SELECT generateUUIDv4()')
replacing_uuid=$($CLICKHOUSE_CLIENT -q 'SELECT generateUUIDv4()')
attach_spoofed_uuid=$($CLICKHOUSE_CLIENT -q 'SELECT generateUUIDv4()')

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ts_05257_external_raw
    (
        id UUID,
        samples Array(Tuple(DateTime64(3), Float64)),
        bucket DateTime64(3),
        min_time DateTime64(3),
        max_time DateTime64(3)
    ) ENGINE = AggregatingMergeTree ORDER BY (id, bucket)
    SETTINGS allow_dimensions_outside_sorting_key = 1
"

expect_error BAD_TYPE_OF_FIELD "
    ATTACH TABLE ts_05257_attach_bad UUID '$attach_raw_uuid'
    ENGINE = TimeSeries SETTINGS version = 7, recent_samples_ttl_seconds = 0
    SAMPLES ts_05257_external_raw
"
expect_error BAD_TYPE_OF_FIELD "
    ATTACH TABLE ts_05257_attach_inner_bad UUID '$attach_inner_uuid'
    ENGINE = TimeSeries SETTINGS version = 7, recent_samples_ttl_seconds = 0
    SAMPLES INNER COLUMNS (samples Array(Tuple(DateTime64(3), Float64)))
    SAMPLES INNER ENGINE = AggregatingMergeTree
"
$CLICKHOUSE_CLIENT -q 'DROP TABLE ts_05257_external_raw'

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ts_05257_external_replacing UUID '$replacing_uuid'
    (
        id UUID,
        samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3), Float64))),
        bucket DateTime64(3),
        min_time SimpleAggregateFunction(min, DateTime64(3)),
        max_time SimpleAggregateFunction(max, DateTime64(3))
    ) ENGINE = ReplacingMergeTree ORDER BY (id, bucket)
"

expect_error INVALID_SETTING_VALUE "
    CREATE TABLE ts_05257_bad ENGINE = TimeSeries
    SETTINGS recent_samples_ttl_seconds = 0 SAMPLES ts_05257_external_replacing
"
# A full ATTACH must check the physical target, not merely the engine declared
# alongside its INNER UUID.
expect_error INVALID_SETTING_VALUE "
    ATTACH TABLE ts_05257_attached_unsafe_inner UUID '$attach_spoofed_uuid'
    ENGINE = TimeSeries SETTINGS version = 7, recent_samples_ttl_seconds = 0
    SAMPLES INNER UUID '$replacing_uuid'
    SAMPLES INNER ENGINE = AggregatingMergeTree
"
$CLICKHOUSE_CLIENT -q 'DROP TABLE ts_05257_external_replacing'

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
inner_good_uuid=$($CLICKHOUSE_CLIENT -q 'SELECT generateUUIDv4()')

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ts_05257_external_raw
    (
        id UUID,
        timestamp DateTime64(3),
        value String
    ) ENGINE = MergeTree ORDER BY (id, timestamp)
"

expect_error BAD_TYPE_OF_FIELD "
    ATTACH TABLE ts_05257_attach_bad UUID '$attach_raw_uuid'
    ENGINE = TimeSeries SETTINGS version = 9, recent_samples_ttl_seconds = 0
    SAMPLES ts_05257_external_raw
"
expect_error BAD_TYPE_OF_FIELD "
    ATTACH TABLE ts_05257_attach_inner_bad UUID '$attach_inner_uuid'
    ENGINE = TimeSeries SETTINGS version = 11, recent_samples_ttl_seconds = 0
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
    SETTINGS version = 11, recent_samples_ttl_seconds = 0
    SAMPLES INNER ENGINE = ReplacingMergeTree ORDER BY (id, bucket)
"
# A full ATTACH must check the physical target, not merely the engine declared
# alongside its INNER UUID.
expect_error INVALID_SETTING_VALUE "
    ATTACH TABLE ts_05257_attached_unsafe_inner UUID '$attach_spoofed_uuid'
    ENGINE = TimeSeries SETTINGS version = 11, recent_samples_ttl_seconds = 0
    SAMPLES INNER UUID '$replacing_uuid'
    SAMPLES INNER ENGINE = AggregatingMergeTree
"
$CLICKHOUSE_CLIENT -q 'DROP TABLE ts_05257_external_replacing'

# Bucketed inner `samples` must retain both samples when two rows are merged.
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ts_05257_inner_good UUID '$inner_good_uuid'
    ENGINE = TimeSeries SETTINGS version = 11, recent_samples_ttl_seconds = 0
    TAGS INNER COLUMNS (id UUID)
"
samples_table=$($CLICKHOUSE_CLIENT -q "
    SELECT name FROM system.tables
    WHERE database = currentDatabase() AND name = '.inner_id.samples.$inner_good_uuid'
")
if [[ -z "$samples_table" ]]; then
    echo 'Cannot find the physical inner samples table' >&2
    exit 1
fi

# `Memory` backups are session-scoped, so keep the backup and all validation
# queries in the same client session. Every unexpected error still fails the test.
$CLICKHOUSE_CLIENT --multiquery -q "
    INSERT INTO \`$samples_table\` (id, samples, bucket, min_time, max_time)
    VALUES ('00000000-0000-0000-0000-000000000001', [(toDateTime64(1000, 3), 1.)], toDateTime64(0, 3), toDateTime64(1000, 3), toDateTime64(1000, 3));
    INSERT INTO \`$samples_table\` (id, samples, bucket, min_time, max_time)
    VALUES ('00000000-0000-0000-0000-000000000001', [(toDateTime64(1001, 3), 2.)], toDateTime64(0, 3), toDateTime64(1001, 3), toDateTime64(1001, 3));
    OPTIMIZE TABLE \`$samples_table\` FINAL;
    SELECT count(), sum(length(samples)) FROM \`$samples_table\`;
    BACKUP TABLE ts_05257_inner_good TO Memory('05257_ts_unsafe_restore_$inner_good_uuid') FORMAT Null;
    ALTER TABLE \`$samples_table\` MODIFY COLUMN samples Array(Tuple(DateTime64(3), Float64));

    -- Revalidate the altered physical inner target, including when restoring into
    -- the existing owner. The backup was taken while the inner target was healthy.
    SELECT * FROM ts_05257_inner_good LIMIT 1; -- { serverError BAD_TYPE_OF_FIELD }
    SELECT 'BAD_TYPE_OF_FIELD';
    INSERT INTO ts_05257_inner_good (metric_name, tags, samples)
    VALUES ('m', map(), [(toDateTime64(1000, 3), 1.)]); -- { serverError BAD_TYPE_OF_FIELD }
    SELECT 'BAD_TYPE_OF_FIELD';
    RESTORE TABLE ts_05257_inner_good FROM Memory('05257_ts_unsafe_restore_$inner_good_uuid')
    SETTINGS allow_non_empty_tables = 1 FORMAT Null; -- { serverError BAD_TYPE_OF_FIELD }
    SELECT 'BAD_TYPE_OF_FIELD';
"
$CLICKHOUSE_CLIENT -q 'DROP TABLE ts_05257_inner_good'

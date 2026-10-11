#!/usr/bin/env bash

# Test: timeSeriesHistogramSelector needs SELECT on the TimeSeries table, also through a table created AS the function,
# even for a user who can read the inner tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_user_05346"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS sel;
DROP TABLE IF EXISTS ts;
DROP USER IF EXISTS $user_name;
SET allow_experimental_time_series_table = 1;
CREATE TABLE ts ENGINE = TimeSeries SETTINGS store_native_histograms = 1;
INSERT INTO ts (metric_name, tags, histograms) VALUES
    ('h', map('job', 'a'), [(toDateTime64(100, 3), 0, 0, 0.001, 5, 7.5, 1, [(0, 2), (1, 1)], [2, 1, 1], [], [], [], 5, 1, [2, 1, 1], [])]);
CREATE TABLE sel AS timeSeriesHistogramSelector(ts, 'h', 0, 200);
CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT CREATE TEMPORARY TABLE ON *.* TO $user_name;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO $user_name;
REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.ts FROM $user_name;
"

# Prints the result, or the grant an ACCESS_DENIED error asks for.
function run_as_user()
{
    local output
    if output=$($CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1); then
        echo "${output:-OK}"
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "$output" | grep -o "the grant [A-Z ]* ON [^ ]*" | head -1 | sed "s/ ${CLICKHOUSE_DATABASE}\./ /; s/\.$//"
    else
        echo "$output"
    fi
}

echo "-- SELECT on every table but the TimeSeries table: the selector is refused"
run_as_user "DESCRIBE TABLE timeSeriesHistogramSelector(ts, 'h', 0, 200) FORMAT Null"
run_as_user "SELECT count() FROM timeSeriesHistogramSelector(ts, 'h', 0, 200)"
run_as_user "SELECT count() FROM timeSeriesHistogramSelector(system.no_such_table_05346, 'h', 0, 200)"
run_as_user "SELECT count() FROM sel"

echo "-- SELECT on the TimeSeries table: the histograms are read"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.ts TO $user_name"
run_as_user "SELECT count() FROM timeSeriesHistogramSelector(ts, 'h', 0, 200)"
run_as_user "SELECT count() FROM sel"

$CLICKHOUSE_CLIENT -q "
DROP USER $user_name;
DROP TABLE sel;
DROP TABLE ts;
"

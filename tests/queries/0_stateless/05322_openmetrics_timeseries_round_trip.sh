#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# `FORMAT OpenMetrics` uses the outer columns of the `TimeSeries` engine, so a table exports with
# `SELECT *` and the exposition reads back with `INSERT ... FORMAT OpenMetrics` into an identical table.

SETTINGS="--allow_experimental_time_series_table=1 --session_timezone=UTC"

$CLICKHOUSE_CLIENT $SETTINGS --multiquery "
    CREATE TABLE ts_src ENGINE = TimeSeries;
    CREATE TABLE ts_dst ENGINE = TimeSeries;

    INSERT INTO ts_src (metric_name, tags, samples, metric_family, type, unit, help) VALUES
        ('http_requests_total', {'method': 'POST', 'code': '200'},
         [(toDateTime64('2024-01-01 00:00:00', 3), 1027), (toDateTime64('2024-01-01 00:00:15.5', 3), 1038)],
         'http_requests', 'counter', '', 'Total number of HTTP requests'),
        ('http_requests_total', {'method': 'GET', 'code': '200'},
         [(toDateTime64('2024-01-01 00:00:00', 3), 34)],
         'http_requests', 'counter', '', 'Total number of HTTP requests'),
        ('temperature_celsius', {'room': 'a'},
         [(toDateTime64('2024-01-01 00:00:00', 3), 21.5)],
         'temperature_celsius', 'gauge', 'celsius', 'Room temperature');
"

QUERY="SELECT * FROM {table:Identifier} ORDER BY metric_family, metric_name, tags FORMAT OpenMetrics"

echo "--- export"
$CLICKHOUSE_CLIENT $SETTINGS --param_table=ts_src --query "$QUERY" | tee "${CLICKHOUSE_TMP}/exported.om"

$CLICKHOUSE_CLIENT $SETTINGS --query "INSERT INTO ts_dst FORMAT OpenMetrics" < "${CLICKHOUSE_TMP}/exported.om"

echo "--- re-export is identical"
$CLICKHOUSE_CLIENT $SETTINGS --param_table=ts_dst --query "$QUERY" | diff - "${CLICKHOUSE_TMP}/exported.om" && echo "OK"

echo "--- imported table"
$CLICKHOUSE_CLIENT $SETTINGS --query "SELECT * FROM ts_dst ORDER BY metric_family, metric_name, tags FORMAT Vertical"

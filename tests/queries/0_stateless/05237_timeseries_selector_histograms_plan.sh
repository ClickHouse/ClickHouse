#!/usr/bin/env bash
# The plan of `timeSeriesSelector` over a TimeSeries table with histograms: the samples table and the histograms table are read
# by one union, the id set of the selector is built once for both of them, and for a selector matching a whole metric the
# probe runs once and the primary-key range on `id` is applied to both tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1"

$CLIENT -q "
    DROP TABLE IF EXISTS ts;
    CREATE TABLE ts ENGINE = TimeSeries;
    INSERT INTO ts (metric_name, tags, samples) VALUES ('m', {'job': 'a'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1)]);
    INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
        VALUES ('m', {'job': 'b'}, [toDateTime64('2026-01-01 00:00:00', 3)], [3], [1.5], [[(0, 1)]], [[3]]);
"

TIME_RANGE="toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)"

echo '--- the id set is built once for both tables ---'
$CLIENT -q "EXPLAIN SELECT * FROM timeSeriesSelector(ts, 'm{job=\"a\"}', $TIME_RANGE)" | grep -c 'CreatingSet'

echo '--- a whole metric: the probe runs once ---'
$CLIENT --send_logs_level debug -q "SELECT count() FROM timeSeriesSelector(ts, 'm', $TIME_RANGE)" 2>&1 \
    | grep -c 'Probing whether selector matches the whole metric'

echo '--- a whole metric: the id range is applied to both tables ---'
$CLIENT -q "EXPLAIN indexes = 1 SELECT * FROM timeSeriesSelector(ts, 'm', $TIME_RANGE)" | grep -c 'Condition: .*id in \[('

$CLIENT -q "DROP TABLE ts"

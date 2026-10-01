#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Test requires ANTLR4, which is disabled in FastTest job.

# For a selector matching a whole metric over a TimeSeries table with histograms, the primary-key range on `id` reaches
# the index analysis of both the samples table and the histograms table. Parallel replicas are disabled: without the local
# plan the reads of both tables are remote and don't show up in the plan.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --enable_parallel_replicas 0"

$CLIENT -q "
    DROP TABLE IF EXISTS ts;
    CREATE TABLE ts ENGINE = TimeSeries;
    INSERT INTO ts (metric_name, tags, samples) VALUES ('m', {'job': 'a'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1)]);
    INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
        VALUES ('m', {'job': 'b'}, [toDateTime64('2026-01-01 00:00:00', 3)], [3], [1.5], [[(0, 1)]], [[3]]);
"

TIME_RANGE="toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)"

echo '--- a whole metric: the id range is applied to both tables ---'
$CLIENT -q "EXPLAIN indexes = 1 SELECT * FROM timeSeriesSelector(ts, 'm', $TIME_RANGE)" | grep -c 'Condition: .*id in \[('

$CLIENT -q "DROP TABLE ts"

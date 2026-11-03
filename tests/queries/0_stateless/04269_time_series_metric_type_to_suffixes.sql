-- Tests for the timeSeriesMetricTypeToSuffixes scalar function.

-- Histogram: bucket/count/sum members.
SELECT timeSeriesMetricTypeToSuffixes('histogram');
-- Gauge histogram: bucket/gcount/gsum members.
SELECT timeSeriesMetricTypeToSuffixes('gaugehistogram');
-- Summary: base/count/sum members.
SELECT timeSeriesMetricTypeToSuffixes('summary');
-- Counter: the base member and the _total member.
SELECT timeSeriesMetricTypeToSuffixes('counter');
-- Info: the base member and the _info member.
SELECT timeSeriesMetricTypeToSuffixes('info');

-- Types with only the family name itself (empty suffix).
SELECT timeSeriesMetricTypeToSuffixes('gauge');
SELECT timeSeriesMetricTypeToSuffixes('unknown');

-- An unrecognized or empty type also yields just the family name.
SELECT timeSeriesMetricTypeToSuffixes('stateset');
SELECT timeSeriesMetricTypeToSuffixes('');

-- Works over a column of types.
SELECT type, timeSeriesMetricTypeToSuffixes(type) AS suffixes
FROM values('type String', 'counter', 'gauge', 'histogram', 'summary')
ORDER BY type;

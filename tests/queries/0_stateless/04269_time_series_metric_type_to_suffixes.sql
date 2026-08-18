-- Tests for the timeSeriesMetricTypeToSuffixes scalar function.

-- Histogram: bucket/count/sum members.
SELECT timeSeriesMetricTypeToSuffixes('histogram');
-- Summary: base/count/sum members.
SELECT timeSeriesMetricTypeToSuffixes('summary');
-- Counter: the _total member.
SELECT timeSeriesMetricTypeToSuffixes('counter');

-- Types with only the family name itself (empty suffix).
SELECT timeSeriesMetricTypeToSuffixes('gauge');
SELECT timeSeriesMetricTypeToSuffixes('info');
SELECT timeSeriesMetricTypeToSuffixes('unknown');

-- An unrecognized or empty type also yields just the family name.
SELECT timeSeriesMetricTypeToSuffixes('gaugehistogram');
SELECT timeSeriesMetricTypeToSuffixes('');

-- Works over a column of types.
SELECT type, timeSeriesMetricTypeToSuffixes(type) AS suffixes
FROM values('type String', 'counter', 'gauge', 'histogram', 'summary')
ORDER BY type;

-- Tests for the timeSeriesMetricTypeToSuffixes scalar function.

-- Histogram: the classic bucket/count/sum members; with `native_histograms` also the native family member.
SELECT timeSeriesMetricTypeToSuffixes('histogram');
SELECT timeSeriesMetricTypeToSuffixes('histogram', true);
-- Gauge histogram: the classic bucket/gcount/gsum members; with `native_histograms` also the native family member.
SELECT timeSeriesMetricTypeToSuffixes('gaugehistogram');
SELECT timeSeriesMetricTypeToSuffixes('gaugehistogram', true);
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

-- The other types don't depend on `native_histograms`, which must be a constant.
SELECT timeSeriesMetricTypeToSuffixes('counter', true);
SELECT timeSeriesMetricTypeToSuffixes('histogram', materialize(true)); -- { serverError ILLEGAL_COLUMN }

-- Works over a column of types.
SELECT type, timeSeriesMetricTypeToSuffixes(type) AS suffixes, timeSeriesMetricTypeToSuffixes(type, true) AS native_suffixes
FROM values('type String', 'counter', 'gauge', 'histogram', 'summary')
ORDER BY type;

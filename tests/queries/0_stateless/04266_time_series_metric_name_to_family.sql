-- Tests for the timeSeriesMetricNameToFamily scalar function.

-- Each of the four canonical suffixes.
SELECT timeSeriesMetricNameToFamily('http_requests_total');
SELECT timeSeriesMetricNameToFamily('http_request_duration_bucket');
SELECT timeSeriesMetricNameToFamily('http_request_duration_count');
SELECT timeSeriesMetricNameToFamily('http_request_duration_sum');

-- No suffix: returned unchanged.
SELECT timeSeriesMetricNameToFamily('cpu_usage');
SELECT timeSeriesMetricNameToFamily('http_requests');

-- Suffix-like substring that doesn't terminate the string: unchanged.
SELECT timeSeriesMetricNameToFamily('total_things');
SELECT timeSeriesMetricNameToFamily('http_total_requests');

-- Only the trailing suffix is stripped, even if multiple appear in the name.
SELECT timeSeriesMetricNameToFamily('http_requests_total_count');
SELECT timeSeriesMetricNameToFamily('foo_sum_total');

-- A name that is exactly a canonical suffix is its own family.
SELECT timeSeriesMetricNameToFamily('_total');
SELECT timeSeriesMetricNameToFamily('_bucket');

-- Empty input: returned unchanged (no suffix matches).
SELECT timeSeriesMetricNameToFamily('');

-- Non-constant metric name.
SELECT name, timeSeriesMetricNameToFamily(name) AS family
FROM
(
    SELECT arrayJoin([
        'requests_total',
        'requests_count',
        'requests_bucket',
        'requests_sum',
        'gauge_no_suffix',
        ''
    ]) AS name
)
ORDER BY name;

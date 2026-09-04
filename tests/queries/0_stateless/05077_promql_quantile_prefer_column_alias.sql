-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Grouped quantile aggregations under prefer_column_name_to_alias = 1.
-- Empty-value rows are filtered after the new_group rename so notEmpty(values)
-- cannot resolve to the pre-aggregation input column.

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;
SET prefer_column_name_to_alias = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

-- 4 series of the metric `m`: hosts h1, h2 in dc=a and hosts h3, h4 in dc=b.
-- Series h4 has a gap at timestamps 110 and 120, and series h1 ends with a NaN sample at timestamp 140.
INSERT INTO prometheus (metric_name, tags, time_series) VALUES
    ('m', map('host', 'h1', 'dc', 'a'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 10), (toDateTime64(120, 3), 4), (toDateTime64(130, 3), 1), (toDateTime64(140, 3), nan)]),
    ('m', map('host', 'h2', 'dc', 'a'), [(toDateTime64(100, 3), 2), (toDateTime64(110, 3), 20), (toDateTime64(120, 3), 3), (toDateTime64(130, 3), 2)]),
    ('m', map('host', 'h3', 'dc', 'b'), [(toDateTime64(100, 3), 3), (toDateTime64(110, 3), 5), (toDateTime64(120, 3), 2), (toDateTime64(130, 3), 3)]),
    ('m', map('host', 'h4', 'dc', 'b'), [(toDateTime64(100, 3), 4), (toDateTime64(130, 3), 4)]);

SELECT '-- quantile by (dc) (0.5, m), range';
SELECT * FROM prometheusQueryRange('prometheus', 'quantile by (dc) (0.5, m)', 100, 140, 10) ORDER BY tags;
SELECT '-- quantile without (host) (0.5, m), range';
SELECT * FROM prometheusQueryRange('prometheus', 'quantile without (host) (0.5, m)', 100, 140, 10) ORDER BY tags;

DROP TABLE prometheus;

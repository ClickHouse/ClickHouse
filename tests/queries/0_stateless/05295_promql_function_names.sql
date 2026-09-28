-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A name followed by '(' is a function name, any other name is a metric name.

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET enable_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('rate', map('job', 'a'), [(toDateTime64(100, 3), 1.5), (toDateTime64(110, 3), 2.5)]),
    ('time', map('job', 'b'), [(toDateTime64(110, 3), 4)]);

SELECT '-- metrics named like functions';
SELECT * FROM prometheusQuery('prometheus', 'rate', 110);
SELECT * FROM prometheusQuery('prometheus', 'time', 110);
SELECT * FROM prometheusQuery('prometheus', 'max_over_time(rate[1m])', 110);

SELECT '-- whitespace before the parenthesis';
SELECT * FROM prometheusQuery('prometheus', 'max_over_time (rate[1m])', 110);
SELECT * FROM prometheusQuery('prometheus', 'min_of (1, 2)', 110);

SELECT '-- unknown functions';
SELECT * FROM prometheusQuery('prometheus', 'foo_bar(rate)', 110); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM prometheusQuery('prometheus', 'Rate(rate[1m])', 110); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM prometheusQuery('prometheus', 'info(rate)', 110); -- { serverError NOT_IMPLEMENTED }

DROP TABLE prometheus;

-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A name without ':' followed by '(' is a function name, any other name is a metric name.

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET enable_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('rate', map('job', 'a'), [(toDateTime64(100, 3), 1.5), (toDateTime64(110, 3), 2.5)]),
    ('time', map('job', 'b'), [(toDateTime64(110, 3), 4)]),
    ('start', map('job', 'c'), [(toDateTime64(110, 3), 5)]),
    ('end', map('job', 'd'), [(toDateTime64(110, 3), 6)]);

SELECT '-- metrics named like functions';
SELECT * FROM prometheusQuery('prometheus', 'rate', 110);
SELECT * FROM prometheusQuery('prometheus', 'time', 110);
SELECT * FROM prometheusQuery('prometheus', 'max_over_time(rate[1m])', 110);

SELECT '-- whitespace before the parenthesis';
SELECT * FROM prometheusQuery('prometheus', 'max_over_time (rate[1m])', 110);
SELECT * FROM prometheusQuery('prometheus', 'min_of (1, 2)', 110);
SELECT * FROM prometheusQuery('prometheus', 'max_over_time\n(rate[1m])', 110);
SELECT * FROM prometheusQuery('prometheus', 'max_over_time # c\r\n(rate[1m])', 110);

SELECT '-- unknown functions';
SELECT * FROM prometheusQuery('prometheus', 'foo_bar(rate)', 110); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM prometheusQuery('prometheus', 'foo_bar (rate)', 110); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM prometheusQuery('prometheus', 'foo_bar\n(rate)', 110); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM prometheusQuery('prometheus', 'foo_bar # c\r\n(rate)', 110); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM prometheusQuery('prometheus', 'Rate(rate[1m])', 110); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM prometheusQuery('prometheus', 'info(rate)', 110); -- { serverError NOT_IMPLEMENTED }

SELECT '-- unknown functions fail before execution, known ones are described';
DESCRIBE prometheusQuery('prometheus', 'foo_bar(rate)', 110); -- { serverError UNKNOWN_FUNCTION }
DESCRIBE prometheusQuery('prometheus', 'abs(foo_bar(rate))', 110); -- { serverError UNKNOWN_FUNCTION }
DESCRIBE prometheusQueryRange('prometheus', 'foo_bar(rate)', 100, 110, 10); -- { serverError UNKNOWN_FUNCTION }
CREATE VIEW unknown_function_view AS SELECT * FROM prometheusQuery('prometheus', 'foo_bar(rate)', 110); -- { serverError UNKNOWN_FUNCTION }
DESCRIBE prometheusQuery('prometheus', 'info(rate)', 110);

SELECT '-- start and end';
SELECT * FROM prometheusQuery('prometheus', 'start()', 110); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery('prometheus', 'end()', 110); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery('prometheus', 'vector(start())', 110); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery('prometheus', 'end() - start()', 110); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery('prometheus', 'START()', 110); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM prometheusQuery('prometheus', 'End()', 110); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM prometheusQuery('prometheus', 'start', 110);
SELECT * FROM prometheusQuery('prometheus', 'end', 110);
SELECT * FROM prometheusQueryRange('prometheus', 'rate @ start()', 100, 110, 10);
SELECT * FROM prometheusQueryRange('prometheus', 'rate @ end ()', 100, 110, 10);
SELECT * FROM prometheusQueryRange('prometheus', 'rate @ START()', 100, 110, 10);

SELECT '-- start, end, range and step return scalars';
DESCRIBE prometheusQuery('prometheus', 'start()', 110);
DESCRIBE prometheusQuery('prometheus', 'end() - start()', 110);
DESCRIBE prometheusQuery('prometheus', 'range()', 110);
DESCRIBE prometheusQuery('prometheus', 'step()', 110);

SELECT '-- limit_ratio is an aggregation operator';
SELECT * FROM prometheusQuery('prometheus', 'limit_ratio(0.5, rate)', 110); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery('prometheus', 'limit_ratio by (job) (0.5, rate)', 110); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery('prometheus', 'limit_ratio(0.5, rate) by (job)', 110); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery('prometheus', 'limit_ratio without (job) (0.5, rate)', 110); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery('prometheus', 'limit_ratio', 110);

DROP TABLE prometheus;

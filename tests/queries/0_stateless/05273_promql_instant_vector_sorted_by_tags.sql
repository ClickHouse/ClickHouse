-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Instant vectors come sorted by tags without an outer ORDER BY, except for topk() and bottomk().

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, samples)
SELECT 'm', map('host', 'h' || toString(number), 'dc', if(number % 2, 'a', 'b')), [(toDateTime64(100, 3), number)]
FROM numbers(8);

SELECT '-- selector';
SELECT * FROM prometheusQuery('prometheus', 'm', 100);
SELECT '-- binary operator';
SELECT * FROM prometheusQuery('prometheus', 'm * 2', 100);
SELECT '-- aggregation';
SELECT * FROM prometheusQuery('prometheus', 'sum by (host) (m)', 100);
SELECT '-- limitk';
SELECT * FROM prometheusQuery('prometheus', 'limitk(8, m)', 100);

SELECT '-- topk and bottomk keep the value order';
SELECT * FROM prometheusQuery('prometheus', 'topk(3, m)', 100);
SELECT * FROM prometheusQuery('prometheus', 'bottomk(3, m)', 100);

DROP TABLE prometheus;

-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET enable_time_series_table = 1;

DROP TABLE IF EXISTS rx;
CREATE TABLE rx ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO rx (metric_name, tags, samples) VALUES
    ('a_total', {'job':'api'}, [(1000, 1.)]),
    ('a_total', {'job':'myweb'}, [(1000, 2.)]),
    ('a_total', {'job':'api-server'}, [(1000, 3.)]),
    ('a_total', {'job':''}, [(1000, 4.)]),
    ('a_total', {'instance':'x'}, [(1000, 5.)]),
    ('a_total', {'job':'web'}, [(1000, 6.)]),
    ('b_total', {'job':'api'}, [(1000, 7.)]),
    ('c_total', {'job':'api'}, [(1000, 8.)]),
    ('a_total_x', {'job':'api'}, [(1000, 9.)]),
    ('xb_total', {'job':'api'}, [(1000, 10.)]);

SELECT '-- literal alternatives match whole values';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job=~"api|web"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job=~"(api|web)"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job=~"(?:api|web)"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job=~"api"}', 1000) ORDER BY value);

SELECT '-- negative matcher returns the complement';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job!~"api|web"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job!~"api"}', 1000) ORDER BY value);

SELECT '-- the number of alternatives is not limited';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx,
    concat('a_total{job=~"', arrayStringConcat(arrayMap(i -> 'x' || toString(i), range(300)), '|'), '|api|web"}'), 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx,
    concat('a_total{job!~"', arrayStringConcat(arrayMap(i -> 'x' || toString(i), range(300)), '|'), '|api|web"}'), 1000) ORDER BY value);

SELECT '-- an empty alternative also matches a missing label';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job=~"api|"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job!~"api|"}', 1000) ORDER BY value);

SELECT '-- a regex that is not a literal list keeps working';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job=~"api.*"}', 1000) ORDER BY value);

SELECT '-- metric name alternatives equal the union of two equality selectors and use the primary key';
SELECT arraySort(groupArray((id, timestamp, value))) = (SELECT arraySort(groupArray((id, timestamp, value))) FROM (
        SELECT * FROM timeSeriesSelector(rx, 'a_total', 0, 2000) UNION ALL SELECT * FROM timeSeriesSelector(rx, 'b_total', 0, 2000))),
    arraySort(groupArray(value))
FROM timeSeriesSelector(rx, '{__name__=~"a_total|b_total"}', 0, 2000);
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"a_total|b_total"}', 0, 2000) SETTINGS force_primary_key = 1;
SELECT count() FROM timeSeriesSelector(rx, '{__name__!~"a_total|b_total", job="api"}', 0, 2000);

DROP TABLE rx;

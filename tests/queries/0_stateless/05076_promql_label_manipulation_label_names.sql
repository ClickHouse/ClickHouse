-- Tags: no-fasttest
-- PromQL needs ANTLR4, which is disabled in the fast-test build.

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('m', map('src', 'value'), [(toDateTime64(100, 3), 1.5), (toDateTime64(110, 3), 2.5)]);

SELECT '-- valid UTF-8 label names are accepted';
SELECT count() FROM prometheusQuery('prometheus', 'label_join(m, "é", "-", "src")', 110);
SELECT count() FROM prometheusQuery('prometheus', 'label_replace(m, "é", "$1", "src", "(.*)")', 110);

SELECT '-- label_replace keeps the empty source-label behavior';
SELECT count() FROM prometheusQuery('prometheus', 'label_replace(m, "dst", "constant", "", ".*")', 110);
SELECT count() FROM prometheusQuery('prometheus', 'label_replace(m, "dst", "constant", "\\xff", ".*")', 110);

SELECT '-- label_replace treats an invalid UTF-8 source label as missing even if a stored tag has that name';
INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('m_invalid', map('\xFF', 'value'), [(toDateTime64(100, 3), 1.5), (toDateTime64(110, 3), 2.5)]);
SELECT arrayMap(t -> (hex(t.1), t.2), tags) FROM prometheusQuery('prometheus', 'label_replace(m_invalid, "dst", "matched", "\\xff", "value")', 110);
SELECT arrayMap(t -> (hex(t.1), t.2), tags) FROM prometheusQuery('prometheus', 'label_replace(m_invalid, "dst", "missing", "\\xff", "")', 110);

SELECT '-- subquery with an empty inner range';
SELECT count() FROM prometheusQuery('prometheus', 'label_join(m, "dst", "-", "src")[1s:15s]', 110);
SELECT count() FROM prometheusQuery('prometheus', 'label_replace(m, "dst", "$1", "src", "(.*)")[1s:15s]', 110);

SELECT '-- invalid label names are rejected';
SELECT * FROM prometheusQuery('prometheus', 'label_join(m, "dst", "-", "\\xff")', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'label_join(m, "\\xff", "-", "src")', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'label_join(m, "", "-", "src")', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'label_replace(m, "\\xff", "$1", "src", ".*")', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'label_replace(m, "", "$1", "src", ".*")', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

DROP TABLE prometheus;

-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: `DatabaseReplicated::dropTable` does not drop `TimeSeries` inner tables synchronously.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, time_series) VALUES
    ('test_metric', map('job', 'foo', 'text', concat('foo', char(10), 'bar'), 'dollar', 'foo$bar', 'quoted', concat('foo', char(92), 'bar'), 'quoted_multiline', concat('[', char(10), 'rest'), 'byte', char(255)), [(toDateTime64(100, 3), 1.)]),
    ('test_metric', map('job', 'bar', 'byte', concat(char(255), 'suffix')),    [(toDateTime64(100, 3), 2.)]),
    ('test_metric', map('job', 'foobar', 'byte', concat('prefix', char(255))), [(toDateTime64(100, 3), 3.)]),
    ('test_metric', map('job', 'xxbar', 'byte', char(254)),  [(toDateTime64(100, 3), 4.)]),
    ('test_metric', map('job', 'xfooy', 'byte', char(255)), [(toDateTime64(100, 3), 5.)]);

SELECT 'positive regex matcher';
SELECT value FROM timeSeriesSelector(ts, '{job=~"foo|bar"}', 0, 1000) ORDER BY value;

SELECT 'negative regex matcher';
SELECT value FROM timeSeriesSelector(ts, '{job!~"foo|bar",__name__=~".+"}', 0, 1000) ORDER BY value;

SELECT 'explicit anchors';
SELECT value FROM timeSeriesSelector(ts, '{job=~"^foo|bar$"}', 0, 1000) ORDER BY value;

SELECT 'dot matches newline';
SELECT value FROM timeSeriesSelector(ts, '{text=~"foo.bar"}', 0, 1000) ORDER BY value;

SELECT 'byte regex matcher';
SELECT value FROM timeSeriesSelector(ts, '{byte=~"\\xFF"}', 0, 1000) ORDER BY value;
SELECT value FROM timeSeriesSelector(ts, '{byte!~"\\xFF"}', 0, 1000) ORDER BY value;

SELECT 'multiline anchors match the whole label';
SELECT count() FROM timeSeriesSelector(ts, '{text=~"(?m)^foo$"}', 0, 1000);
SELECT count() FROM timeSeriesSelector(ts, '{text!~"(?m)^foo$"}', 0, 1000);

SELECT 'quoted literal keeps a following multiline flag visible';
SELECT count() FROM timeSeriesSelector(ts, '{quoted_multiline=~"\\\\Q[\\\\E(?m)$"}', 0, 1000);
SELECT count() FROM timeSeriesSelector(ts, '{quoted_multiline!~"\\\\Q[\\\\E(?m)$"}', 0, 1000);

SELECT 'literal escapes keep the whole-value anchor';
SELECT count() FROM timeSeriesSelector(ts, '{dollar=~"foo\\\\$"}', 0, 1000);
SELECT count() FROM timeSeriesSelector(ts, '{dollar!~"foo\\\\$"}', 0, 1000);
SELECT count() FROM timeSeriesSelector(ts, '{dollar=~"\\\\Qfoo$"}', 0, 1000);
SELECT count() FROM timeSeriesSelector(ts, '{dollar!~"\\\\Qfoo$"}', 0, 1000);
SELECT count() FROM timeSeriesSelector(ts, '{dollar=~"\\\\Qfoo"}', 0, 1000);
SELECT count() FROM timeSeriesSelector(ts, '{dollar!~"\\\\Qfoo"}', 0, 1000);
SELECT 'quoted literal with a backslash before \\E';
SELECT count() FROM timeSeriesSelector(ts, '{quoted=~"\\\\Qfoo\\\\\\\\Ebar$"}', 0, 1000);
SELECT count() FROM timeSeriesSelector(ts, '{quoted!~"\\\\Qfoo\\\\\\\\Ebar$"}', 0, 1000);

SELECT 'quoted literal punctuation keeps alternation anchored';
SELECT count() FROM timeSeriesSelector(ts, '{job=~"\\\\Q(\\\\Efoo|bar"}', 0, 1000);
SELECT count() FROM timeSeriesSelector(ts, '{job!~"\\\\Q(\\\\Efoo|bar"}', 0, 1000);

SELECT 'POSIX character class punctuation keeps alternation anchored';
SELECT count() FROM timeSeriesSelector(ts, '{job=~"[[:alpha:](]foo|bar"}', 0, 1000);
SELECT count() FROM timeSeriesSelector(ts, '{job!~"[[:alpha:](]foo|bar"}', 0, 1000);

SELECT 'common-prefix alternation stays whole-value anchored';
SELECT value FROM timeSeriesSelector(ts, '{job=~"foo|fooz"}', 0, 1000) ORDER BY value;
SELECT value FROM timeSeriesSelector(ts, '{job!~"foo|fooz"}', 0, 1000) ORDER BY value;

SELECT 'invalid trailing escape keeps the regexp error';
SELECT count() FROM timeSeriesSelector(ts, '{dollar=~"foo\\\\"}', 0, 1000); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT count() FROM timeSeriesSelector(ts, '{dollar!~"foo\\\\"}', 0, 1000); -- { serverError CANNOT_COMPILE_REGEXP }

SELECT 'invalid regex syntax keeps the regexp error';
SELECT count() FROM timeSeriesSelector(ts, '{job=~"*bar"}', 0, 1000); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT count() FROM timeSeriesSelector(ts, '{job!~"*bar"}', 0, 1000); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT count() FROM timeSeriesSelector(ts, '{job=~"foo|)("}', 0, 1000); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT count() FROM timeSeriesSelector(ts, '{job!~"foo|)("}', 0, 1000); -- { serverError CANNOT_COMPILE_REGEXP }

SELECT 'quantified leading anchor keeps whole-value matching';
SELECT value FROM timeSeriesSelector(ts, '{job=~"^?bar"}', 0, 1000) ORDER BY value;
SELECT value FROM timeSeriesSelector(ts, '{job!~"^?bar"}', 0, 1000) ORDER BY value;

SELECT 'metric regex with a character class';
SELECT count() FROM timeSeriesSelector(ts, '{__name__=~"http_[0-9]+"}', 0, 1000);

SELECT 'metric regex with a leading class terminator';
SELECT count() FROM timeSeriesSelector(ts, '{__name__=~"foo[](?m)]"}', 0, 1000);

SELECT 'metric regex with a POSIX class';
SELECT count() FROM timeSeriesSelector(ts, '{__name__=~"foo[[:alpha:](?m)]"}', 0, 1000);

SELECT 'simple metric regex keeps the tags primary-key optimization';
-- EXPLAIN of timeSeriesSelector only shows the samples-side read. Inspect the tags table directly,
-- where the plain anchored regex and its primary-key condition are visible.
SELECT plan LIKE '%Filter column: match(metric_name, \'^test_metric$\')%'
    AND plan LIKE '%Condition: (metric_name in [\'test_metric\', \'test_metric\'])%'
    AND NOT plan LIKE '%^(?:test_metric)$%' AS has_plain_metric_regex
FROM (SELECT arrayStringConcat(groupArray(explain), '\\n') AS plan
      FROM (EXPLAIN indexes = 1 SELECT metric_name FROM timeSeriesTags(ts) WHERE match(metric_name, '^test_metric$')));

SELECT 'deeply nested matcher regex stays safe';
SELECT count() FROM timeSeriesSelector(
    ts,
    concat('{job=~"', repeat('(', 10000), 'foo', repeat(')', 10000), '"}'),
    0, 1000);

SELECT 'metric regex matcher';
SELECT count() FROM timeSeriesSelector(ts, '{__name__=~"test_metric"}', 0, 1000);
SELECT count() FROM timeSeriesSelector(ts, '{__name__!~"test_metric"}', 0, 1000);

DROP TABLE ts;

-- Tags: no-fasttest, no-replicated-database
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL
-- grammar requires it. The experimental TimeSeries table engine does not
-- round-trip through DatabaseReplicated and the cleanup query hangs.
--
-- Exercise Prometheus/PromQL parsing (Parsers/Prometheus/PrometheusQueryParsingUtil.cpp
-- and the ANTLR visitor) via the prometheusQuery() / prometheusQueryRange() table
-- functions against an empty TimeSeries table. This drives tryParseScalar,
-- tryUnescapeStringLiteral and the ANTLR grammar end-to-end.
--
-- Scope note: scalar / arithmetic / unary / bool-comparison / vector / scalar
-- expressions return concrete values that genuinely exercise the evaluator.
-- The selector-based queries (rate, offset, label matchers, by/without,
-- group_left/right, prometheusQueryRange) only assert count() == 0 because the
-- TimeSeries engine does not support direct INSERT (data only enters via the
-- Prometheus remote-write HTTP path). They still cover the *parser* surface,
-- which is the goal here; an evaluator regression on an empty selector would
-- need a separate integration test.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

SELECT '--- scalar expressions ---';
SELECT value FROM prometheusQuery('ts', '1 + 2', 1000);
SELECT value FROM prometheusQuery('ts', '10 * 5', 1000);
SELECT value FROM prometheusQuery('ts', '10 - 3', 1000);
SELECT value FROM prometheusQuery('ts', '10 / 4', 1000);
SELECT value FROM prometheusQuery('ts', '2 ^ 5', 1000);
SELECT value FROM prometheusQuery('ts', '10 % 3', 1000);

SELECT '--- unary operators ---';
SELECT value FROM prometheusQuery('ts', '-5', 1000);
SELECT value FROM prometheusQuery('ts', '+3.14', 1000);

SELECT '--- bool comparison with bool modifier ---';
SELECT value FROM prometheusQuery('ts', '1 > bool 2', 1000);
SELECT value FROM prometheusQuery('ts', '2 >= bool 2', 1000);
SELECT value FROM prometheusQuery('ts', '1 == bool 1', 1000);
SELECT value FROM prometheusQuery('ts', '1 != bool 2', 1000);
SELECT value FROM prometheusQuery('ts', '3 < bool 4', 1000);
SELECT value FROM prometheusQuery('ts', '3 <= bool 3', 1000);

SELECT '--- tryParseScalar: scientific / hex / infinity / NaN ---';
SELECT value FROM prometheusQuery('ts', '1.5e3', 1000);
SELECT value FROM prometheusQuery('ts', '0x1F', 1000);
SELECT value FROM prometheusQuery('ts', 'Inf', 1000);
SELECT value FROM prometheusQuery('ts', '+Inf', 1000);
SELECT value FROM prometheusQuery('ts', '-Inf', 1000);
SELECT value FROM prometheusQuery('ts', 'NaN', 1000);

SELECT '--- vector() and scalar() ---';
SELECT value FROM prometheusQuery('ts', 'vector(42)', 1000);
SELECT value FROM prometheusQuery('ts', 'scalar(vector(7))', 1000);

SELECT '--- duration variants: ms, s, m, h, d, w, y ---';
SELECT count() FROM prometheusQuery('ts', 'rate(up[500ms])', 1000);
SELECT count() FROM prometheusQuery('ts', 'rate(up[30s])', 1000);
SELECT count() FROM prometheusQuery('ts', 'rate(up[5m])', 1000);
SELECT count() FROM prometheusQuery('ts', 'rate(up[1h])', 1000);
SELECT count() FROM prometheusQuery('ts', 'rate(up[2d])', 1000);
SELECT count() FROM prometheusQuery('ts', 'rate(up[1w])', 1000);
SELECT count() FROM prometheusQuery('ts', 'rate(up[1y])', 1000);

SELECT '--- compound durations (1h30m / 1y6mo-like) ---';
SELECT count() FROM prometheusQuery('ts', 'rate(up[1h30m])', 1000);

SELECT '--- offsets, positive and negative ---';
SELECT count() FROM prometheusQuery('ts', 'up offset 5m', 1000);
SELECT count() FROM prometheusQuery('ts', 'up offset -5m', 1000);

SELECT '--- label matchers (=, !=, =~, !~) ---';
SELECT count() FROM prometheusQuery('ts', 'up{job="myjob"}', 1000);
SELECT count() FROM prometheusQuery('ts', 'up{job!="myjob"}', 1000);
SELECT count() FROM prometheusQuery('ts', 'up{job=~".*"}', 1000);
SELECT count() FROM prometheusQuery('ts', 'up{job!~"ignore"}', 1000);

SELECT '--- aggregations with by / without ---';
SELECT count() FROM prometheusQuery('ts', 'sum by (job) (up)', 1000);
SELECT count() FROM prometheusQuery('ts', 'sum without (instance) (up)', 1000);
SELECT count() FROM prometheusQuery('ts', 'topk(3, up)', 1000);
SELECT count() FROM prometheusQuery('ts', 'quantile(0.5, up)', 1000);

SELECT '--- binary ops with group / ignoring / on ---';
SELECT count() FROM prometheusQuery('ts', 'up * on (job) up', 1000);
SELECT count() FROM prometheusQuery('ts', 'up * ignoring (instance) up', 1000);
SELECT count() FROM prometheusQuery('ts', 'up * on (job) group_left() up', 1000);
SELECT count() FROM prometheusQuery('ts', 'up * ignoring (instance) group_right() up', 1000);

SELECT '--- prometheusQueryRange with various steps ---';
SELECT count() FROM prometheusQueryRange('ts', 'up', 1000, 2000, 60);
SELECT count() FROM prometheusQueryRange('ts', 'up', 1000, 2000, 1);

SELECT '--- error: invalid syntax ---';
SELECT * FROM prometheusQuery('ts', 'not valid!!!', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'rate(up[])', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'rate(up[abc])', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', '1 +', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'up{job=}', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'up{job="unclosed}', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }

DROP TABLE ts;

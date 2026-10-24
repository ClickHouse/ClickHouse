-- Tags: no-fasttest, no-parallel-replicas, long
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the test asserts on the query plan shape (the number of reads of the samples table).
-- Tag long: the file runs many PromQL statements.

-- The SQL generated from a PromQL query may reference a named subquery more than once: the left side of `or`
-- feeds both the per-group presence-mask step and the final merge join, and the topk/bottomk/limitk operand grid
-- feeds both the group-selecting aggregation and the value-masking join. Such subqueries must be marked
-- AS MATERIALIZED and evaluated once, while subqueries referenced once must stay inlined and must not be materialized.

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

-- 4 series of the metric `m`: hosts h1, h2 in dc=a and hosts h3, h4 in dc=b,
-- and 2 series of the metric `n`: host h5 in dc=a and host h6 in dc=b.
-- Series h4 has a gap at timestamps 110 and 120.
INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1', 'dc', 'a'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 10), (toDateTime64(120, 3), 4), (toDateTime64(130, 3), 1)]),
    ('m', map('host', 'h2', 'dc', 'a'), [(toDateTime64(100, 3), 2), (toDateTime64(110, 3), 20), (toDateTime64(120, 3), 3), (toDateTime64(130, 3), 2)]),
    ('m', map('host', 'h3', 'dc', 'b'), [(toDateTime64(100, 3), 3), (toDateTime64(110, 3), 5), (toDateTime64(120, 3), 2), (toDateTime64(130, 3), 3)]),
    ('m', map('host', 'h4', 'dc', 'b'), [(toDateTime64(100, 3), 4), (toDateTime64(130, 3), 4)]),
    ('n', map('host', 'h5', 'dc', 'a'), [(toDateTime64(100, 3), 7), (toDateTime64(110, 3), 7), (toDateTime64(120, 3), 7), (toDateTime64(130, 3), 7)]),
    ('n', map('host', 'h6', 'dc', 'b'), [(toDateTime64(100, 3), 8), (toDateTime64(110, 3), 8), (toDateTime64(120, 3), 8), (toDateTime64(130, 3), 8)]),
    ('sparse_or_left', map('test_case', 'or'), [(toDateTime64(100, 3), 1), (toDateTime64(130, 3), 4)]),
    ('dense_or_right', map('test_case', 'or'), [(toDateTime64(100, 3), 7), (toDateTime64(110, 3), 7), (toDateTime64(120, 3), 7), (toDateTime64(130, 3), 7)]),
    ('dense_and_left', map('test_case', 'and'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 1), (toDateTime64(120, 3), 1), (toDateTime64(130, 3), 1)]),
    ('sparse_and_right', map('test_case', 'and'), [(toDateTime64(100, 3), 2), (toDateTime64(130, 3), 2)]),
    ('dense_unless_left', map('test_case', 'unless'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 1), (toDateTime64(120, 3), 1), (toDateTime64(130, 3), 1)]),
    ('sparse_unless_right', map('test_case', 'unless'), [(toDateTime64(100, 3), 2), (toDateTime64(130, 3), 2)]),
    ('or_chain_a', map('test_case', 'chain'), [(toDateTime64(100, 3), 1)]),
    ('or_chain_b', map('test_case', 'chain'), [(toDateTime64(100, 3), 9), (toDateTime64(110, 3), 2)]),
    ('or_chain_c', map('test_case', 'chain'), [(toDateTime64(100, 3), 8), (toDateTime64(110, 3), 8), (toDateTime64(120, 3), 3)]);

INSERT INTO prometheus (metric_name, tags, samples)
SELECT
    metric_name,
    map('series', toString(series)),
    groupArray((toDateTime64(100 + step * 10, 3), toFloat64(1)))
FROM
(
    SELECT 'wide_left' AS metric_name, number AS series, step
    FROM numbers(200)
    ARRAY JOIN arrayFilter(s -> s % 3 != 0, range(30)) AS step
    UNION ALL
    SELECT 'wide_right' AS metric_name, number AS series, step
    FROM numbers(200)
    ARRAY JOIN arrayFilter(s -> s % 5 != 0, range(30)) AS step
)
GROUP BY metric_name, series;

SELECT '-- or, range';
SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(n[10])', 100, 130, 10) ORDER BY tags;
SELECT '-- or, range: identical result with materialization disabled';
-- the analyzer warns that MATERIALIZED is ignored with the setting disabled
SET send_logs_level = 'error';
SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(n[10])', 100, 130, 10) ORDER BY tags SETTINGS enable_materialized_cte = 0;
SET send_logs_level = 'warning';

SELECT '-- topk(2), range';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(2, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- topk(2), range: identical result with materialization disabled';
SET send_logs_level = 'error';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(2, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags SETTINGS enable_materialized_cte = 0;
SET send_logs_level = 'warning';

SELECT '-- sparse or preserves both sides at different steps';
SELECT count() AS series_count, sum(length(samples)) AS sample_count
FROM prometheusQueryRange(
    'prometheus',
    'last_over_time(sparse_or_left[10]) or on(test_case) last_over_time(dense_or_right[10])',
    100, 130, 10);

SELECT '-- sparse and keeps only right-present steps';
SELECT count() AS series_count, sum(length(samples)) AS sample_count
FROM prometheusQueryRange(
    'prometheus',
    'last_over_time(dense_and_left[10]) and on(test_case) last_over_time(sparse_and_right[10])',
    100, 130, 10);

SELECT '-- sparse unless keeps only right-absent steps';
SELECT count() AS series_count, sum(length(samples)) AS sample_count
FROM prometheusQueryRange(
    'prometheus',
    'last_over_time(dense_unless_left[10]) unless on(test_case) last_over_time(sparse_unless_right[10])',
    100, 130, 10);

SELECT '-- empty left vector remains empty for and';
SELECT count() AS series_count
FROM prometheusQueryRange(
    'prometheus',
    'last_over_time(missing_metric[10]) and last_over_time(dense_and_left[10])',
    100, 130, 10);

SELECT '-- empty right vector leaves unless left unchanged';
SELECT count() AS series_count, sum(length(samples)) AS sample_count
FROM prometheusQueryRange(
    'prometheus',
    'last_over_time(dense_unless_left[10]) unless last_over_time(missing_metric[10])',
    100, 130, 10);

SELECT '-- or: the left side is evaluated once: two samples reads in total (left materialized + right inlined)';
SELECT countIf(explain LIKE '%ReadFromMergeTree%.inner_id.samples.%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(n[10])', 100, 130, 10));

SELECT '-- and uses a compact per-step presence mask';
SELECT countIf(explain LIKE '%groupBitOrForEach%') > 0 AS uses_presence_mask,
       countIf(explain LIKE '%countForEach%') = 0 AS avoids_count_aggregate
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) and last_over_time(n[10])', 100, 130, 10));

SELECT '-- or uses a compact per-step presence mask';
SELECT countIf(explain LIKE '%groupBitOrForEach%') > 0 AS uses_presence_mask,
       countIf(explain LIKE '%countForEach%') = 0 AS avoids_count_aggregate
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(n[10])', 100, 130, 10));

SELECT '-- unless uses a compact per-step presence mask';
SELECT countIf(explain LIKE '%groupBitOrForEach%') > 0 AS uses_presence_mask,
       countIf(explain LIKE '%countForEach%') = 0 AS avoids_count_aggregate
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) unless last_over_time(n[10])', 100, 130, 10));

SELECT '-- subqueries referenced once stay inlined: plain sum(rate(...)) reads the samples table once, nothing is materialized';
SELECT countIf(explain LIKE '%ReadFromMergeTree%.inner_id.samples.%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'sum(rate(m[20]))', 100, 130, 10));

SELECT '-- subqueries referenced once stay inlined: a binary operator reads each of its two operands once, nothing is materialized';
SELECT countIf(explain LIKE '%ReadFromMergeTree%.inner_id.samples.%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'sum by (dc) (m) / on (dc) count by (dc) (m)', 130));

-- The streaming topk plan reads its operand grid twice (the group-selecting aggregation and the join
-- masking the non-selected values), so the grid is materialized and the samples table is read once.
SELECT '-- topk: the operand grid is read by two steps, so it is materialized; one samples read';
SELECT countIf(explain LIKE '%ReadFromMergeTree%.inner_id.samples.%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'topk(2, last_over_time(m[10]))', 100, 130, 10));

SELECT '-- explicitly disabling the setting restores the inlined plan: the left side of `or` is read twice again';
SET enable_materialized_cte = 0;
SET send_logs_level = 'error';
SELECT countIf(explain LIKE '%ReadFromMergeTree%.inner_id.samples.%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(n[10])', 100, 130, 10));
SET send_logs_level = 'warning';
SET enable_materialized_cte = 1;

SELECT '-- three-term or on(test_case) keeps the earliest sample at each step';
SELECT tags, samples
FROM prometheusQueryRange(
    'prometheus',
    'last_over_time(or_chain_a[10]) or on(test_case) last_over_time(or_chain_b[10]) or on(test_case) last_over_time(or_chain_c[10])',
    100, 120, 10)
ORDER BY tags;

SELECT '-- thirteen-term or returns every operand';
SELECT count() AS series_count
FROM prometheusQuery(
    'prometheus',
    'label_replace(vector(1), "k", "v1", "", "") or label_replace(vector(2), "k", "v2", "", "") or label_replace(vector(3), "k", "v3", "", "") or label_replace(vector(4), "k", "v4", "", "") or label_replace(vector(5), "k", "v5", "", "") or label_replace(vector(6), "k", "v6", "", "") or label_replace(vector(7), "k", "v7", "", "") or label_replace(vector(8), "k", "v8", "", "") or label_replace(vector(9), "k", "v9", "", "") or label_replace(vector(10), "k", "v10", "", "") or label_replace(vector(11), "k", "v11", "", "") or label_replace(vector(12), "k", "v12", "", "") or label_replace(vector(13), "k", "v13", "", "")',
    100);

SELECT '-- empty on() keeps the earliest sample at each step';
SELECT tags, samples
FROM prometheusQueryRange(
    'prometheus',
    'last_over_time(or_chain_a[10]) or on() last_over_time(or_chain_b[10]) or on() last_over_time(or_chain_c[10])',
    100, 120, 10)
ORDER BY tags;

SELECT '-- identical series or matches the left operand';
SELECT count() = 0
FROM
(
    SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(m[10])', 100, 130, 10)
    EXCEPT
    SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(m[10])', 100, 130, 10)
);

SELECT '-- or range records peak memory';
SET log_queries = 1;
SELECT count() FROM prometheusQueryRange(
    'prometheus',
    'last_over_time(or_chain_a[10]) or on() last_over_time(or_chain_b[10])',
    100, 120, 10)
SETTINGS log_comment = 'promql_or_empty_on';
SELECT count() FROM prometheusQueryRange(
    'prometheus',
    'last_over_time(m[10]) or last_over_time(m[10])',
    100, 130, 10)
SETTINGS log_comment = 'promql_or_identical';
SYSTEM FLUSH LOGS query_log;
SELECT
    countIf(log_comment = 'promql_or_empty_on' AND type = 'QueryFinish' AND memory_usage > 0) > 0,
    countIf(log_comment = 'promql_or_identical' AND type = 'QueryFinish' AND memory_usage > 0) > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND current_database = currentDatabase()
  AND log_comment IN ('promql_or_empty_on', 'promql_or_identical');

SELECT '-- wide empty on() keeps left series and unmasked right steps';
SELECT count() AS series_count, sum(length(samples)) AS sample_count
FROM prometheusQueryRange(
    'prometheus',
    'last_over_time(wide_left[10]) or on() last_over_time(wide_right[10])',
    100, 390, 10)
SETTINGS log_comment = 'promql_or_wide_on';

SELECT '-- wide identical series or matches the left operand';
SELECT count() = 0
FROM
(
    SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(wide_left[10]) or last_over_time(wide_left[10])', 100, 390, 10)
    EXCEPT
    SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(wide_left[10])', 100, 390, 10)
)
SETTINGS log_comment = 'promql_or_wide_identical';

SYSTEM FLUSH LOGS query_log;
SELECT
    countIf(log_comment = 'promql_or_wide_on' AND type = 'QueryFinish' AND memory_usage > 0 AND query_duration_ms >= 0) > 0,
    countIf(log_comment = 'promql_or_wide_identical' AND type = 'QueryFinish' AND memory_usage > 0 AND query_duration_ms >= 0) > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND current_database = currentDatabase()
  AND log_comment IN ('promql_or_wide_on', 'promql_or_wide_identical');

SELECT '-- thirteen-term or names each operand once in the shared body';
SELECT sum(countMatches(explain, '"v1"')) AS v1_mentions,
       sum(countMatches(explain, '"v13"')) AS v13_mentions
FROM (EXPLAIN AST SELECT * FROM prometheusQuery(
    'prometheus',
    'label_replace(vector(1), "k", "v1", "", "") or label_replace(vector(2), "k", "v2", "", "") or label_replace(vector(3), "k", "v3", "", "") or label_replace(vector(4), "k", "v4", "", "") or label_replace(vector(5), "k", "v5", "", "") or label_replace(vector(6), "k", "v6", "", "") or label_replace(vector(7), "k", "v7", "", "") or label_replace(vector(8), "k", "v8", "", "") or label_replace(vector(9), "k", "v9", "", "") or label_replace(vector(10), "k", "v10", "", "") or label_replace(vector(11), "k", "v11", "", "") or label_replace(vector(12), "k", "v12", "", "") or label_replace(vector(13), "k", "v13", "", "")',
    100));

SELECT '-- thirteen-term or with materialization disabled exceeds the query tree limit';
SET send_logs_level = 'none';
SELECT count()
FROM prometheusQuery(
    'prometheus',
    'label_replace(vector(1), "k", "v1", "", "") or label_replace(vector(2), "k", "v2", "", "") or label_replace(vector(3), "k", "v3", "", "") or label_replace(vector(4), "k", "v4", "", "") or label_replace(vector(5), "k", "v5", "", "") or label_replace(vector(6), "k", "v6", "", "") or label_replace(vector(7), "k", "v7", "", "") or label_replace(vector(8), "k", "v8", "", "") or label_replace(vector(9), "k", "v9", "", "") or label_replace(vector(10), "k", "v10", "", "") or label_replace(vector(11), "k", "v11", "", "") or label_replace(vector(12), "k", "v12", "", "") or label_replace(vector(13), "k", "v13", "", "")',
    100)
SETTINGS enable_materialized_cte = 0; -- { serverError BAD_ARGUMENTS }
SET send_logs_level = 'warning';

DROP TABLE prometheus;

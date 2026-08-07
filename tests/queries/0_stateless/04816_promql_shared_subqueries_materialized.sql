-- Tags: no-fasttest, no-replicated-database, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: `DatabaseReplicated::dropTable` does not drop `TimeSeries` inner tables synchronously, so the deferred inner DROPs are rejected with "ON CLUSTER is not allowed for Replicated database".
-- Tag no-parallel-replicas: the test asserts on the query plan shape (the number of reads of the samples table).

-- The SQL generated from a PromQL query may reference a named subquery more than once: the left side of `or`
-- feeds both the per-group counting step and the final merge join. Such subqueries must be marked AS MATERIALIZED
-- and evaluated once, while subqueries referenced once must stay inlined and must not be materialized.

DROP TABLE IF EXISTS tags_table;
DROP TABLE IF EXISTS samples_table;
DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE tags_table
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE samples_table
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE prometheus ENGINE = TimeSeries
SAMPLES samples_table TAGS tags_table;

-- 4 series of the metric `m`: hosts h1, h2 in dc=a and hosts h3, h4 in dc=b,
-- and 2 series of the metric `n`: host h5 in dc=a and host h6 in dc=b.
INSERT INTO tags_table (id, metric_name, tags, min_time, max_time) VALUES
    (1, 'm', map('host', 'h1', 'dc', 'a'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (2, 'm', map('host', 'h2', 'dc', 'a'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (3, 'm', map('host', 'h3', 'dc', 'b'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (4, 'm', map('host', 'h4', 'dc', 'b'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (5, 'n', map('host', 'h5', 'dc', 'a'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (6, 'n', map('host', 'h6', 'dc', 'b'), toDateTime64(0, 3), toDateTime64(1000, 3));

-- Series h4 has a gap at timestamps 110 and 120.
INSERT INTO samples_table (id, timestamp, value) VALUES
    (1, toDateTime64(100, 3), 1), (1, toDateTime64(110, 3), 10), (1, toDateTime64(120, 3), 4), (1, toDateTime64(130, 3), 1),
    (2, toDateTime64(100, 3), 2), (2, toDateTime64(110, 3), 20), (2, toDateTime64(120, 3), 3), (2, toDateTime64(130, 3), 2),
    (3, toDateTime64(100, 3), 3), (3, toDateTime64(110, 3), 5), (3, toDateTime64(120, 3), 2), (3, toDateTime64(130, 3), 3),
    (4, toDateTime64(100, 3), 4), (4, toDateTime64(130, 3), 4),
    (5, toDateTime64(100, 3), 7), (5, toDateTime64(110, 3), 7), (5, toDateTime64(120, 3), 7), (5, toDateTime64(130, 3), 7),
    (6, toDateTime64(100, 3), 8), (6, toDateTime64(110, 3), 8), (6, toDateTime64(120, 3), 8), (6, toDateTime64(130, 3), 8);

SELECT '-- or, range';
SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(n[10])', 100, 130, 10) ORDER BY tags;
SELECT '-- or, range: identical result with materialization disabled';
SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(n[10])', 100, 130, 10) ORDER BY tags SETTINGS enable_materialized_cte = 0;

SELECT '-- topk(2), range';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(2, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- topk(2), range: identical result with materialization disabled';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(2, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags SETTINGS enable_materialized_cte = 0;

SELECT '-- or: the left side is evaluated once: two samples reads in total (left materialized + right inlined)';
SELECT countIf(explain LIKE '%ReadFromMergeTree%samples_table%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(n[10])', 100, 130, 10));

SELECT '-- subqueries referenced once stay inlined: plain sum(rate(...)) reads the samples table once, nothing is materialized';
SELECT countIf(explain LIKE '%ReadFromMergeTree%samples_table%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'sum(rate(m[20]))', 100, 130, 10));

SELECT '-- subqueries referenced once stay inlined: a binary operator reads each of its two operands once, nothing is materialized';
SELECT countIf(explain LIKE '%ReadFromMergeTree%samples_table%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'sum by (dc) (m) / on (dc) count by (dc) (m)', 130));

-- The current topk plan is a linear chain which references its operand subquery once, so nothing is materialized
-- in it. (If a plan re-reading the operand is introduced - e.g. a streaming topk - the reference counting will
-- pick it up automatically and this will become `1	1`.)
SELECT '-- topk: the operand subquery is referenced once today, so it stays inlined';
SELECT countIf(explain LIKE '%ReadFromMergeTree%samples_table%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'topk(2, last_over_time(m[10]))', 100, 130, 10));

SELECT '-- explicitly disabling the setting restores the inlined plan: the left side of `or` is read twice again';
SET enable_materialized_cte = 0;
SELECT countIf(explain LIKE '%ReadFromMergeTree%samples_table%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(n[10])', 100, 130, 10));

DROP TABLE prometheus;
DROP TABLE tags_table;
DROP TABLE samples_table;

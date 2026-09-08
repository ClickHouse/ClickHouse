-- Tags: no-fasttest
-- Tag no-fasttest: PromQL requires ANTLR4.
SET allow_experimental_time_series_table = 1;
SET allow_experimental_time_series_aggregate_functions = 1;
SET enable_materialized_cte = 0;
SET session_timezone = 'UTC';
SET max_block_size = 17;

CREATE TABLE ts_materialized_ids ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));
INSERT INTO ts_materialized_ids (metric_name, tags, time_series)
SELECT concat('m', toString(n)), map('series', toString(number), 'env', if(number % 2, 'dev', 'prod')),
    [(toDateTime64(100, 3), toFloat64(number))]
FROM numbers(512) ARRAY JOIN [1, 128, 129, 512] AS n WHERE number < n;

-- Empty, single-ID, inline-set, and materialized-set selectors, split across scan blocks.
SELECT count(), sum(value) FROM timeSeriesSelector(ts_materialized_ids, 'missing', 0, 200);
SELECT count(), sum(value) FROM timeSeriesSelector(ts_materialized_ids, 'm1', 0, 200);
SELECT count(), sum(value) FROM timeSeriesSelector(ts_materialized_ids, 'm128', 0, 200);
SELECT count(), sum(value) FROM timeSeriesSelector(ts_materialized_ids, 'm129', 0, 200);
SELECT count(), sum(value) FROM timeSeriesSelector(ts_materialized_ids, 'm512', 0, 200);
SELECT count(), sum(value) FROM timeSeriesSelector(ts_materialized_ids, 'm512{env="prod"}', 0, 200);
SELECT count() FROM timeSeriesSelector(ts_materialized_ids, 'm512{env="missing"}', 0, 200);
SELECT count() FROM timeSeriesSelector(ts_materialized_ids, 'm512', 201, 300);

-- The samples plan reads the samples table; the tags were already read during selector resolution.
SELECT countSubstrings(arrayStringConcat(groupArray(explain)), 'ReadFromMergeTree')
FROM (EXPLAIN SELECT sum(value) FROM timeSeriesSelector(ts_materialized_ids, 'm512', 0, 200));
SELECT countSubstrings(arrayStringConcat(groupArray(explain)), 'ReadFromMergeTree')
FROM (EXPLAIN SELECT sum(value) FROM timeSeriesSelector(ts_materialized_ids, 'm512{env="prod"}', 0, 200));

-- Reused CTEs, IN sets, and separate PromQL selectors retain their own IDs and stored tags.
SET enable_materialized_cte = 1;
WITH samples AS MATERIALIZED (SELECT id, value FROM timeSeriesSelector(ts_materialized_ids, 'm512', 0, 200))
SELECT count(), sum(value) FROM samples WHERE id IN (SELECT id FROM samples WHERE value % 2 = 0);
WITH samples AS MATERIALIZED (SELECT value FROM timeSeriesSelector(ts_materialized_ids, 'm512{env="prod"}', 0, 200))
SELECT result FROM (SELECT sum(value) AS result FROM samples UNION ALL SELECT toFloat64(count()) FROM samples) ORDER BY result;
SELECT * FROM prometheusQuery(ts_materialized_ids, 'sum(m512) + sum(m512)', 100) ORDER BY ALL;
SELECT * FROM prometheusQuery(ts_materialized_ids, 'sum by (env) (m512)', 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts_materialized_ids, 'sum(m512{env="prod"}) / sum(m512)', 100, 200, 100) ORDER BY ALL;
DROP TABLE ts_materialized_ids;

-- Typed literals preserve fixed-width IDs, including their binary string components.
CREATE TABLE ts_materialized_ids ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, FixedString(16)));
INSERT INTO ts_materialized_ids (metric_name, tags, time_series)
SELECT 'm', map('series', toString(number)), [(toDateTime64(100, 3), toFloat64(number))] FROM numbers(128);
SELECT count(), sum(value) FROM timeSeriesSelector(ts_materialized_ids, 'm', 0, 200);
SELECT * FROM prometheusQuery(ts_materialized_ids, 'm{series="127"}', 100) ORDER BY ALL;
DROP TABLE ts_materialized_ids;

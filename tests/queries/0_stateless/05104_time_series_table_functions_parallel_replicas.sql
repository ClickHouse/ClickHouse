-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Reading through `timeSeriesTags`, `timeSeriesSamples` and `timeSeriesMetrics` with parallel replicas.
-- The test runs in its own database, so the replicas must receive the table name qualified with the database,
-- and every replica must announce its parts to the coordinator instead of reading the whole table.
-- See https://github.com/ClickHouse/ClickHouse/issues/118130

SET allow_experimental_time_series_table = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 0;
SET automatic_parallel_replicas_mode = 0;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, time_series) VALUES ('m1', {'job': 'j1'}, [(1, 1.), (2, 2.)]), ('m2', {'job': 'j2'}, [(1, 3.)]);
INSERT INTO ts (metric_family, type, unit, help) VALUES ('m1', 'gauge', '', 'help for m1'), ('m2', 'counter', '', 'help for m2');

SELECT '-- one-argument form';
SELECT metric_name FROM timeSeriesTags(ts) ORDER BY metric_name;
SELECT value FROM timeSeriesSamples(ts) ORDER BY value;
SELECT metric_family_name, type FROM timeSeriesMetrics(ts) ORDER BY metric_family_name;

SELECT '-- every row is returned once';
SELECT count() FROM (SELECT * FROM timeSeriesTags(currentDatabase(), ts)) SETTINGS optimize_trivial_count_query = 0;
SELECT count() FROM (SELECT * FROM timeSeriesSamples(currentDatabase(), ts)) SETTINGS optimize_trivial_count_query = 0;
SELECT count() FROM (SELECT * FROM timeSeriesMetrics(currentDatabase(), ts)) SETTINGS optimize_trivial_count_query = 0;

SELECT '-- the same with a local plan';
SELECT count() FROM (SELECT * FROM timeSeriesTags(ts)) SETTINGS optimize_trivial_count_query = 0, parallel_replicas_local_plan = 1;
SELECT count() FROM (SELECT * FROM timeSeriesSamples(ts)) SETTINGS optimize_trivial_count_query = 0, parallel_replicas_local_plan = 1;

-- FINAL is not supported with parallel replicas: the query is rejected when they are forced and runs without them otherwise.
SELECT '-- FINAL';
SELECT count() FROM (SELECT * FROM timeSeriesTags(ts) FINAL) SETTINGS optimize_trivial_count_query = 0; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM (SELECT * FROM timeSeriesTags(ts) FINAL) SETTINGS optimize_trivial_count_query = 0, enable_parallel_replicas = 1;

-- A JOIN with a MergeTree table on the left side is sent to the replicas as a whole,
-- so the table functions on the right side must get the table name qualified with the database too.
SELECT '-- table functions on the right side of a JOIN';
CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mt VALUES (1), (2), (3);
SELECT count() FROM mt AS l INNER JOIN timeSeriesSamples(ts) AS r ON l.x = toUInt64(r.value);
SELECT count() FROM mt AS l INNER JOIN timeSeriesSelector(ts, 'm1', toDateTime64(0, 3), toDateTime64(10, 3)) AS r ON l.x = toUInt64(r.value);
SELECT count() FROM mt AS l INNER JOIN prometheusQuery(ts, 'm1', toDateTime64(10, 3)) AS r ON l.x = toUInt64(r.value);
SELECT count() FROM mt AS l INNER JOIN (SELECT toUInt64(arrayMax(x -> x.2, time_series)) AS x FROM prometheusQueryRange(ts, 'm1', toDateTime64(0, 3), toDateTime64(10, 3), INTERVAL 1 SECOND)) AS r ON l.x = r.x;

DROP TABLE mt;
DROP TABLE ts;

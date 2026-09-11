-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Reading through the `TimeSeries` table functions with parallel replicas.
-- The test runs in its own database, so a replica that resolves the table name against its own current
-- database reads nothing, and every replica must announce its parts to the coordinator instead of
-- reading the whole table.
-- See https://github.com/ClickHouse/ClickHouse/issues/118130

SET allow_experimental_time_series_table = 1;
-- Pinned because the body asserts analyzer-side behaviour: the `FINAL` refusal below is raised by the
-- planner, which the old analyzer does not reach, and the coordinated read it checks for is the one the
-- planner anchors. The legacy path has its own subcase further down, which sets `enable_analyzer = 0`
-- explicitly so it is covered whichever analyzer the job defaults to.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 0;
SET automatic_parallel_replicas_mode = 0;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, time_series) VALUES ('m1', {'job': 'j1'}, [(1, 1.), (2, 2.)]), ('m2', {'job': 'j2'}, [(1, 3.)]);
INSERT INTO ts (metric_family, type, unit, help) VALUES ('m1', 'gauge', '', 'help for m1'), ('m2', 'counter', '', 'help for m2');

-- The table name is not qualified, so it only resolves on the replicas if they are told which database
-- the query was written against.
SELECT '-- one-argument form';
SELECT metric_name FROM timeSeriesTags(ts) ORDER BY metric_name;
SELECT value FROM timeSeriesSamples(ts) ORDER BY value;
SELECT metric_family_name, type FROM timeSeriesMetrics(ts) ORDER BY metric_family_name;

-- Each replica must read only its own share of the parts.
SELECT '-- every row is returned once';
SELECT count() FROM (SELECT * FROM timeSeriesTags(ts)) SETTINGS optimize_trivial_count_query = 0;
SELECT count() FROM (SELECT * FROM timeSeriesSamples(ts)) SETTINGS optimize_trivial_count_query = 0;
SELECT count() FROM (SELECT * FROM timeSeriesMetrics(ts)) SETTINGS optimize_trivial_count_query = 0;

SELECT '-- the same with a local plan';
SELECT count() FROM (SELECT * FROM timeSeriesTags(ts)) SETTINGS optimize_trivial_count_query = 0, parallel_replicas_local_plan = 1;
SELECT count() FROM (SELECT * FROM timeSeriesSamples(ts)) SETTINGS optimize_trivial_count_query = 0, parallel_replicas_local_plan = 1;

-- The read is coordinated over a query text rather than a shipped plan, so it must stay correct with
-- `serialize_query_plan` on as well.
SELECT '-- the same with serialize_query_plan';
SELECT count() FROM (SELECT * FROM timeSeriesTags(ts)) SETTINGS optimize_trivial_count_query = 0, serialize_query_plan = 1;
SELECT count() FROM (SELECT * FROM timeSeriesSamples(ts)) SETTINGS optimize_trivial_count_query = 0, serialize_query_plan = 1;
SELECT count() FROM (SELECT * FROM timeSeriesSamples(ts)) SETTINGS optimize_trivial_count_query = 0, serialize_query_plan = 1, parallel_replicas_local_plan = 1;

-- The replicas resolve the name the same way whichever analyzer plans the query.
SELECT '-- the same without the analyzer';
SELECT count() FROM (SELECT * FROM timeSeriesTags(ts)) SETTINGS optimize_trivial_count_query = 0, enable_analyzer = 0;
SELECT count() FROM (SELECT * FROM timeSeriesSamples(ts)) SETTINGS optimize_trivial_count_query = 0, enable_analyzer = 0;

-- Correct results alone would not show that the read was coordinated: an initiator that quietly gave up
-- on parallel replicas also returns each row once. Assert that the query was announced to more than one
-- replica. (`ParallelReplicasUsedCount` would be a stronger claim, but it counts the replicas the
-- coordinator actually handed marks to, which for a table this small is one whatever the planner did.)
SELECT '-- the read is announced to the other replicas';
SELECT count() FROM (SELECT * FROM timeSeriesSamples(ts))
    SETTINGS optimize_trivial_count_query = 0, log_comment = '05175_used_replicas';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParallelReplicasAvailableCount'] > 1
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05175_used_replicas' AND type = 'QueryFinish' AND is_initial_query
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- The custom-key modes fan out through a different path: it addresses the read by its own table id and
-- builds the remote context with a different helper, so neither the table identity nor the database
-- travelled with the read until both were fixed. The key has to be numeric, and `id` is a tuple.
-- `serialize_query_plan` is pinned off because a custom key is refused outright when it is on, and the
-- job that ships plans turns it on for every query.
SELECT '-- custom key';
SELECT count() FROM (SELECT * FROM timeSeriesSamples(ts))
    SETTINGS optimize_trivial_count_query = 0, enable_parallel_replicas = 1, serialize_query_plan = 0,
             parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'cityHash64(id)';
SELECT count() FROM (SELECT * FROM timeSeriesTags(ts))
    SETTINGS optimize_trivial_count_query = 0, enable_parallel_replicas = 1, serialize_query_plan = 0,
             parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'cityHash64(id)';

-- FINAL is not supported with parallel replicas: the query is rejected when they are forced and runs
-- without them otherwise.
SELECT '-- FINAL';
SELECT count() FROM (SELECT * FROM timeSeriesTags(ts) FINAL) SETTINGS optimize_trivial_count_query = 0; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM (SELECT * FROM timeSeriesTags(ts) FINAL) SETTINGS optimize_trivial_count_query = 0, enable_parallel_replicas = 1;

-- A JOIN with a `MergeTree` table on the left side is sent to the replicas as a whole, so the table
-- functions on the right side have to resolve there too.
SELECT '-- table functions on the right side of a JOIN';
CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mt VALUES (1), (2), (3);
SELECT count() FROM mt AS l INNER JOIN timeSeriesSamples(ts) AS r ON l.x = toUInt64(r.value);
SELECT count() FROM mt AS l INNER JOIN timeSeriesSelector(ts, 'm1', toDateTime64(0, 3), toDateTime64(10, 3)) AS r ON l.x = toUInt64(r.value);
SELECT count() FROM mt AS l INNER JOIN prometheusQuery(ts, 'm1', toDateTime64(10, 3)) AS r ON l.x = toUInt64(r.value);
SELECT count() FROM mt AS l INNER JOIN (SELECT toUInt64(arrayMax(x -> x.2, time_series)) AS x FROM prometheusQueryRange(ts, 'm1', toDateTime64(0, 3), toDateTime64(10, 3), INTERVAL 1 SECOND)) AS r ON l.x = r.x;

-- A table function that builds a storage of its own is not a reference to a table that every replica
-- has, so it must not drive the read. `timeSeriesSelector` reads through the `TimeSeries` table but
-- returns a storage of its own, so it stays on a single replica and still returns each row once.
SELECT '-- a table function that is not a plain table reference';
SELECT count() FROM (SELECT * FROM timeSeriesSelector(ts, 'm1', toDateTime64(0, 3), toDateTime64(10, 3))) SETTINGS optimize_trivial_count_query = 0;

DROP TABLE mt;
DROP TABLE ts;

-- Tags: no-fasttest
-- Tag no-fasttest: the `TimeSeries` engine is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET max_threads = 1;
SET any_join_distinct_right_table_keys = 0;
SET query_plan_enable_optimizations = 1;
SET explain_query_plan_default = 'pretty';
SET join_algorithm = 'grace_hash';
SET grace_hash_join_initial_buckets = 2;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS ts_join_settings;
CREATE TABLE ts_join_settings ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
TAGS INNER ENGINE = Memory;
INSERT INTO ts_join_settings (metric_name, tags, time_series, metric_family, type)
VALUES ('m', {'n': 'a'}, [('2024-01-01 00:00:00', 1)], 'm', 'gauge');
INSERT INTO ts_join_settings (metric_name, tags, time_series)
VALUES ('m', {'n': 'a'}, [('2024-01-01 00:00:01', 2)]);

-- Both internal joins must use the requested algorithm.
SELECT countIf(explain LIKE '%Algorithm: GraceHashJoin%') = 2
FROM (EXPLAIN PLAN actions = 1 SELECT * FROM ts_join_settings);

-- The outer column types and complete samples remain valid with these caller settings.
SET join_use_nulls = 1;
SET aggregate_functions_null_for_empty = 1;
SELECT metric_name, tags['n'], arraySort(time_series), metric_family, unit, help, type
FROM ts_join_settings FINAL;

SET join_algorithm = 'full_sorting_merge';
SELECT metric_name, tags['n'], arraySort(time_series), metric_family, unit, help, type
FROM ts_join_settings FINAL;

-- The legacy `ANY` semantics also return each series once with all its samples.
SET join_algorithm = 'hash';
SET any_join_distinct_right_table_keys = 1;
SELECT metric_name, tags['n'], arraySort(time_series), metric_family, unit, help, type
FROM ts_join_settings FINAL;

DROP TABLE ts_join_settings;

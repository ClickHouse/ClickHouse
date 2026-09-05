-- Index analysis was lost when the parallel replicas settings were set in a subquery instead of at top level.
-- The outer settings only keep the test runner from enabling parallel replicas for the whole query.

CREATE TABLE t_05045 (a UInt64, ts DateTime)
ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (a, ts);

INSERT INTO t_05045 SELECT 1, toDateTime('2026-07-19 00:00:00') + toIntervalDay(number % 32) FROM numbers(3200);
INSERT INTO t_05045 SELECT 1, toDateTime('2026-08-20 00:00:00') + toIntervalSecond(number) FROM numbers(20000);

SELECT countIf(explain LIKE '%Condition: true%') AS indexes_not_used
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM (SELECT a FROM t_05045 WHERE ts >= '2026-08-20 00:00:00'
    SETTINGS enable_parallel_replicas = 1,
             parallel_replicas_for_non_replicated_merge_tree = 1,
             cluster_for_parallel_replicas = 'parallel_replicas',
             max_parallel_replicas = 3,
             parallel_replicas_local_plan = 1,
             parallel_replicas_min_number_of_rows_per_replica = 1000)
)
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

DROP TABLE t_05045;

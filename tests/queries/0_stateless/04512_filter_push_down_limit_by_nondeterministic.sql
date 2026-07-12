-- Test: a non-deterministic predicate on the LIMIT BY key must NOT be pushed below LimitBy.
-- The push is gated by step_changes_the_number_of_rows=true, forbidding non-deterministic
-- conjuncts; otherwise a per-row non-deterministic predicate would run before the per-group
-- top-N and change which rows survive. enable_parallel_replicas=0 so the pushed predicate
-- becomes the local read condition (generateSnowflakeID is per-row non-deterministic).

DROP TABLE IF EXISTS t_04512;
SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;

DROP TABLE IF EXISTS t_04512;
CREATE TABLE t_04512 (key String, ts DateTime, val UInt64)
ENGINE = MergeTree ORDER BY (key, ts)
AS SELECT toString(number % 100) AS key, toDateTime(number) AS ts, number AS val
FROM numbers(100000);
OPTIMIZE TABLE t_04512 FINAL;

-- Deterministic key predicate: fully pushed below LimitBy, no Filter step remains above.
SELECT countIf(explain LIKE '%Filter (%') AS filters_above
FROM (
    EXPLAIN
    SELECT * FROM (
        SELECT key, ts, val FROM t_04512 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE key = '5' SETTINGS enable_parallel_replicas = 0
);

-- Non-deterministic key predicate: NOT pushed, a Filter step stays above LimitBy.
SELECT countIf(explain LIKE '%Filter (%') AS filters_above
FROM (
    EXPLAIN
    SELECT * FROM (
        SELECT key, ts, val FROM t_04512 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE key < toString(generateSnowflakeID()) SETTINGS enable_parallel_replicas = 0
);

DROP TABLE t_04512;

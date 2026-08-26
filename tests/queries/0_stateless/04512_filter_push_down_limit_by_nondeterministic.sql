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

-- filter_above_limitby=1 iff a Filter step exists AND appears above LimitBy in the
-- plan (its plan line precedes LimitBy's). This distinguishes "Filter above LimitBy"
-- from "LimitBy above Filter": if the predicate were (wrongly) pushed below LimitBy,
-- the Filter line would follow LimitBy and this flag would flip to 0 -- merely counting
-- Filter nodes cannot tell the two placements apart.

-- Deterministic key predicate: fully pushed below LimitBy, no Filter step remains above -> 0.
SELECT (has_filter AND fl < ll) AS filter_above_limitby
FROM (
    SELECT countIf(explain LIKE '%Filter (%') > 0 AS has_filter,
           minIf(ln, explain LIKE '%Filter (%') AS fl,
           minIf(ln, explain LIKE '%LimitBy%')  AS ll
    FROM (
        SELECT explain, rowNumberInAllBlocks() AS ln
        FROM (
            EXPLAIN
            SELECT * FROM (
                SELECT key, ts, val FROM t_04512 ORDER BY key, ts LIMIT 1 BY key
            ) WHERE key = '5' SETTINGS enable_parallel_replicas = 0
        )
    )
);

-- Non-deterministic key predicate: NOT pushed, a Filter step stays above LimitBy -> 1.
SELECT (has_filter AND fl < ll) AS filter_above_limitby
FROM (
    SELECT countIf(explain LIKE '%Filter (%') > 0 AS has_filter,
           minIf(ln, explain LIKE '%Filter (%') AS fl,
           minIf(ln, explain LIKE '%LimitBy%')  AS ll
    FROM (
        SELECT explain, rowNumberInAllBlocks() AS ln
        FROM (
            EXPLAIN
            SELECT * FROM (
                SELECT key, ts, val FROM t_04512 ORDER BY key, ts LIMIT 1 BY key
            ) WHERE key < toString(generateSnowflakeID()) SETTINGS enable_parallel_replicas = 0
        )
    )
);

DROP TABLE t_04512;

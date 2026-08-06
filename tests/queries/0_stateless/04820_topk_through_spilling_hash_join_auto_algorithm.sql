-- Tags: no-parallel-replicas
-- Assertions on reading do not hold with parallel replicas.

-- Mirror of `04670_topk_through_spilling_hash_join_left_join` with `join_algorithm = 'auto'`.
-- With an effective spill threshold, the `AUTO` branch of the physicalization returns
-- `SpillingHashJoin` (never `JoinSwitcher` - `MergeJoin::isSupported` implies
-- `GraceHashJoin::isSupported`, so the spilling branch always fires first), which is pinnable.
-- `topKThroughJoin` therefore treats `AUTO` like a plain spill-capable hash join and defers to
-- reading in order through the join, instead of pushing its own `Sort` and `Limit` down.

DROP TABLE IF EXISTS t_topk_join_auto_events;
DROP TABLE IF EXISTS t_topk_join_auto_payloads;

CREATE TABLE t_topk_join_auto_events (Time DateTime, Id String) ENGINE = MergeTree ORDER BY Time
SETTINGS index_granularity = 8192;
INSERT INTO t_topk_join_auto_events
    SELECT toDateTime('2024-01-01 00:00:00') + INTERVAL number SECOND AS Time, toString(number) AS Id
    FROM numbers(500000);

CREATE TABLE t_topk_join_auto_payloads (Payload String, Id String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_topk_join_auto_payloads
    SELECT concat('Payload ', toString(number)) AS Payload, toString(number) AS Id
    FROM numbers(30) WHERE number % 4 = 0;

SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000;
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;
SET read_in_order_use_virtual_row = 1, query_plan_optimize_join_order_limit = 1;
-- `query_plan_join_swap_table = 0` is a pre-existing requirement of the `topKThroughJoin`
-- deferral, not just plan stability: under the default `auto` a later optimization may swap
-- the join sides, so the deferral never commits and the pushed-down plan is kept.
SET query_plan_join_swap_table = 0, join_algorithm = 'auto';
SET max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 1;

-- No `Sorting` below the `Join`: `topKThroughJoin` deferred, and the left table is read in order.
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT t_topk_join_auto_events.Time, t_topk_join_auto_events.Id, t_topk_join_auto_payloads.Payload
    FROM t_topk_join_auto_events
    LEFT JOIN t_topk_join_auto_payloads ON t_topk_join_auto_events.Id = t_topk_join_auto_payloads.Id
    ORDER BY t_topk_join_auto_events.Time
    LIMIT 3
-- `Expression` steps are named differently by the old analyzer, and they carry no information here.
) WHERE explain NOT LIKE '%Expression%'
  AND (explain LIKE '%Sorting%' OR explain LIKE '%Join%' OR explain LIKE '%ReadType%');

SELECT t_topk_join_auto_events.Time, t_topk_join_auto_events.Id, t_topk_join_auto_payloads.Payload
FROM t_topk_join_auto_events
LEFT JOIN t_topk_join_auto_payloads ON t_topk_join_auto_events.Id = t_topk_join_auto_payloads.Id
ORDER BY t_topk_join_auto_events.Time
LIMIT 3
SETTINGS log_comment = '04820_topk_auto_keep_left_order';

SYSTEM FLUSH LOGS query_log;

-- Pinned to the in-memory algorithm, so it never spilled despite the 1-byte threshold.
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS spills
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '04820_topk_auto_keep_left_order'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- With no effective spill threshold, `AUTO` may still physicalize to `JoinSwitcher`, which is
-- not pinnable, so the deferral must stay disabled: `topKThroughJoin` pushes its own `Sort`
-- and `Limit` down to the preserved side (the `Sorting` step below the `Join`).
SET max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 0;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT t_topk_join_auto_events.Time, t_topk_join_auto_events.Id, t_topk_join_auto_payloads.Payload
    FROM t_topk_join_auto_events
    LEFT JOIN t_topk_join_auto_payloads ON t_topk_join_auto_events.Id = t_topk_join_auto_payloads.Id
    ORDER BY t_topk_join_auto_events.Time
    LIMIT 3
) WHERE explain NOT LIKE '%Expression%'
  AND (explain LIKE '%Sorting%' OR explain LIKE '%Join%' OR explain LIKE '%ReadType%');

DROP TABLE t_topk_join_auto_events;
DROP TABLE t_topk_join_auto_payloads;

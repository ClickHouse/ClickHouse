-- Tags: no-parallel-replicas
-- Assertions on reading do not hold with parallel replicas.

-- `query_plan_read_in_order_through_spilling_join = 0` restores the conservative behaviour: a join
-- that may spill is never used for reading in order, so `topKThroughJoin` does not step aside and
-- pushes its own `Sort` and `Limit` down to the preserved side instead.
-- The mirror image of `04670_topk_through_spilling_hash_join_left_join`, which runs the same query
-- with the setting enabled (the default).
--
-- Unlike `04670`, the join here really does spill, so the threshold and the right table have to be
-- sized realistically: `GraceHashJoin` inherits the same threshold and splits buckets until each one
-- fits in half of it, so a degenerate threshold would make it double the bucket count past
-- `grace_hash_join_max_buckets` and throw `LIMIT_EXCEEDED`.

DROP TABLE IF EXISTS t_topk_no_join_events;
DROP TABLE IF EXISTS t_topk_no_join_payloads;

CREATE TABLE t_topk_no_join_events (Time DateTime, Id String) ENGINE = MergeTree ORDER BY Time
SETTINGS index_granularity = 8192;
INSERT INTO t_topk_no_join_events
    SELECT toDateTime('2024-01-01 00:00:00') + INTERVAL number SECOND AS Time, toString(number) AS Id
    FROM numbers(500000);

CREATE TABLE t_topk_no_join_payloads (Payload String, Id String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_topk_no_join_payloads
    SELECT concat('Payload ', toString(number)) AS Payload, toString(number) AS Id
    FROM numbers(1000000) WHERE number % 4 = 0;

SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000;
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;
SET read_in_order_use_virtual_row = 1, query_plan_optimize_join_order_limit = 1;
-- `query_plan_join_swap_table = 0` is a pre-existing requirement of the `topKThroughJoin`
-- deferral, not just plan stability: under the default `auto` a later optimization may swap
-- the join sides, so the deferral never commits and the pushed-down plan is kept.
SET query_plan_join_swap_table = 0, join_algorithm = 'hash';
SET max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 4000000;
SET grace_hash_join_initial_buckets = 1, grace_hash_join_max_buckets = 1024;
SET query_plan_read_in_order_through_spilling_join = 0;

-- `topKThroughJoin` pushed its own `Sort` and `Limit` below the `Join`.
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT t_topk_no_join_events.Time, t_topk_no_join_events.Id, t_topk_no_join_payloads.Payload
    FROM t_topk_no_join_events
    LEFT JOIN t_topk_no_join_payloads ON t_topk_no_join_events.Id = t_topk_no_join_payloads.Id
    ORDER BY t_topk_no_join_events.Time
    LIMIT 3
-- `Expression` steps are named differently by the old analyzer, and they carry no information here.
) WHERE explain NOT LIKE '%Expression%'
  AND (explain LIKE '%Sorting%' OR explain LIKE '%Join%' OR explain LIKE '%ReadType%');

-- The results are the same either way.
SELECT t_topk_no_join_events.Time, t_topk_no_join_events.Id, t_topk_no_join_payloads.Payload
FROM t_topk_no_join_events
LEFT JOIN t_topk_no_join_payloads ON t_topk_no_join_events.Id = t_topk_no_join_payloads.Id
ORDER BY t_topk_no_join_events.Time
LIMIT 3
SETTINGS log_comment = '04671_topk_no_keep_left_order';

SYSTEM FLUSH LOGS query_log;

-- Nothing pinned the join, so the threshold made it spill.
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0 AS spilled
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '04671_topk_no_keep_left_order'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_topk_no_join_events;
DROP TABLE t_topk_no_join_payloads;

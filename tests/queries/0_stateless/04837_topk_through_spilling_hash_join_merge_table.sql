-- Tags: no-parallel-replicas
-- Assertions on reading do not hold with parallel replicas.

-- Same handoff as `04670`, but the preserved side reads from a `Merge` table.
--
-- `topKThroughJoin` steps aside when reading in order through the join is available. That
-- availability is decided by `optimizeReadInOrder`, which supports `ReadFromMergeTree`,
-- `ReadFromMerge` and `ReadFromObjectStorageStep` alike, so the deferral probe has to consider
-- all of them: probing only `ReadFromMergeTree` kept the pushed-down `Sort` and `Limit` for a
-- `Merge` table even though reading in order was possible.
--
-- The threshold below is 1 byte, so a join that was free to spill certainly would.

DROP TABLE IF EXISTS t_topk_merge_events_1;
DROP TABLE IF EXISTS t_topk_merge_events_2;
DROP TABLE IF EXISTS t_topk_merge_events;
DROP TABLE IF EXISTS t_topk_merge_payloads;

CREATE TABLE t_topk_merge_events_1 (Time DateTime, Id String) ENGINE = MergeTree ORDER BY Time
SETTINGS index_granularity = 8192;
CREATE TABLE t_topk_merge_events_2 (Time DateTime, Id String) ENGINE = MergeTree ORDER BY Time
SETTINGS index_granularity = 8192;

INSERT INTO t_topk_merge_events_1
    SELECT toDateTime('2024-01-01 00:00:00') + INTERVAL number SECOND AS Time, toString(number) AS Id
    FROM numbers(250000);
INSERT INTO t_topk_merge_events_2
    SELECT toDateTime('2024-01-01 00:00:00') + INTERVAL (number + 250000) SECOND AS Time,
           toString(number + 250000) AS Id
    FROM numbers(250000);

CREATE TABLE t_topk_merge_events AS t_topk_merge_events_1
ENGINE = Merge(currentDatabase(), '^t_topk_merge_events_[12]$');

CREATE TABLE t_topk_merge_payloads (Payload String, Id String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_topk_merge_payloads
    SELECT concat('Payload ', toString(number)) AS Payload, toString(number) AS Id
    FROM numbers(30) WHERE number % 4 = 0;

SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000;
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;
SET read_in_order_use_virtual_row = 1, query_plan_optimize_join_order_limit = 1;
-- `query_plan_join_swap_table = 0` is a pre-existing requirement of the `topKThroughJoin`
-- deferral: under the default `auto` a later optimization may swap the join sides, so the
-- deferral never commits and the pushed-down plan is kept.
SET query_plan_join_swap_table = 0, join_algorithm = 'hash';
SET max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 1;

-- No `Sorting` below the `Join`: `topKThroughJoin` deferred, and every table behind the
-- `Merge` table is read in order.
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT t_topk_merge_events.Time, t_topk_merge_events.Id, t_topk_merge_payloads.Payload
    FROM t_topk_merge_events
    LEFT JOIN t_topk_merge_payloads ON t_topk_merge_events.Id = t_topk_merge_payloads.Id
    ORDER BY t_topk_merge_events.Time
    LIMIT 3
-- `Expression` steps are named differently by the old analyzer, and they carry no information here.
) WHERE explain NOT LIKE '%Expression%'
  AND (explain LIKE '%Sorting%' OR explain LIKE '%Join%' OR explain LIKE '%ReadType%');

SELECT t_topk_merge_events.Time, t_topk_merge_events.Id, t_topk_merge_payloads.Payload
FROM t_topk_merge_events
LEFT JOIN t_topk_merge_payloads ON t_topk_merge_events.Id = t_topk_merge_payloads.Id
ORDER BY t_topk_merge_events.Time
LIMIT 3
SETTINGS log_comment = '04837_topk_merge_keep_left_order';

SYSTEM FLUSH LOGS query_log;

-- Pinned to the in-memory algorithm, so it never spilled despite the 1-byte threshold.
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS spills
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '04837_topk_merge_keep_left_order'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- With the compatibility setting off the join is not pinned, so reading in order through it is
-- unavailable and `topKThroughJoin` keeps its own pushed-down `Sort` and `Limit` instead.
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT t_topk_merge_events.Time, t_topk_merge_events.Id, t_topk_merge_payloads.Payload
    FROM t_topk_merge_events
    LEFT JOIN t_topk_merge_payloads ON t_topk_merge_events.Id = t_topk_merge_payloads.Id
    ORDER BY t_topk_merge_events.Time
    LIMIT 3
    SETTINGS query_plan_read_in_order_through_spilling_join = 0
) WHERE explain NOT LIKE '%Expression%'
  AND (explain LIKE '%Sorting%' OR explain LIKE '%Join%' OR explain LIKE '%ReadType%');

DROP TABLE t_topk_merge_events;
DROP TABLE t_topk_merge_events_1;
DROP TABLE t_topk_merge_events_2;
DROP TABLE t_topk_merge_payloads;

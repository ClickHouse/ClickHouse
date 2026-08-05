-- Tags: no-parallel-replicas
-- Assertions on reading do not hold with parallel replicas.

-- The aggregation-in-order pass walks through a join that may spill (see
-- `04648_spilling_hash_join_keep_left_order`), but when the `GROUP BY` keys are not compatible
-- with the left sorting key, no in-order aggregation is installed, and the join must not be
-- pinned in memory: it has to keep its ability to spill. This is a regression test for a bug
-- where the join was pinned even though `buildInputOrderFromUnorderedKeys` produced no order,
-- so such queries lost spilling for no benefit.
--
-- The join here really does spill, so the threshold and the right table are sized as in
-- `04649_spilling_hash_join_keep_left_order_opt_out`: `GraceHashJoin` inherits the same
-- threshold and splits buckets until each one fits in half of it, so a degenerate threshold
-- would make it double the bucket count past `grace_hash_join_max_buckets` and throw
-- `LIMIT_EXCEEDED`.

DROP TABLE IF EXISTS t_agg_no_order_events;
DROP TABLE IF EXISTS t_agg_no_order_payloads;

CREATE TABLE t_agg_no_order_events (Time DateTime, Id String) ENGINE = MergeTree ORDER BY Time
SETTINGS index_granularity = 8192;
INSERT INTO t_agg_no_order_events
    SELECT toDateTime('2024-01-01 00:00:00') + INTERVAL number SECOND AS Time, toString(number) AS Id
    FROM numbers(500000);

CREATE TABLE t_agg_no_order_payloads (Payload String, Id String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_agg_no_order_payloads
    SELECT concat('Payload ', toString(number)) AS Payload, toString(number) AS Id
    FROM numbers(1000000) WHERE number % 4 = 0;

SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET optimize_aggregation_in_order = 1;
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;
SET read_in_order_use_virtual_row = 1, query_plan_optimize_join_order_limit = 1;
SET query_plan_join_swap_table = 0, join_algorithm = 'hash';
SET max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 4000000;
SET grace_hash_join_initial_buckets = 1, grace_hash_join_max_buckets = 1024;
SET query_plan_read_in_order_through_spilling_join = 1;

-- `Id` is not a prefix of the left sorting key (`Time`), so aggregation in order does not apply
-- and nothing is read in order.
SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT t_agg_no_order_events.Id, count()
    FROM t_agg_no_order_events
    INNER JOIN t_agg_no_order_payloads ON t_agg_no_order_events.Id = t_agg_no_order_payloads.Id
    GROUP BY t_agg_no_order_events.Id
) WHERE explain LIKE '%ReadType%';

SELECT count() FROM (
    SELECT t_agg_no_order_events.Id
    FROM t_agg_no_order_events
    INNER JOIN t_agg_no_order_payloads ON t_agg_no_order_events.Id = t_agg_no_order_payloads.Id
    GROUP BY t_agg_no_order_events.Id
)
SETTINGS log_comment = '04760_agg_no_order_still_spills';

SYSTEM FLUSH LOGS query_log;

-- No in-order aggregation was installed, so nothing pinned the join, and the threshold made it spill.
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0 AS spilled
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '04760_agg_no_order_still_spills'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_agg_no_order_events;
DROP TABLE t_agg_no_order_payloads;

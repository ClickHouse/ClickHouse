-- Tags: no-parallel-replicas
-- Regression test for a false-positive INDEX_NOT_USED (error 277) thrown when a query reads through a
-- view with parallel replicas and force_index_by_date / force_primary_key enabled. See issue #108266.
--
-- Under parallel replicas a table read through a view is planned as the view's own inner query, so the
-- outer WHERE predicate stays in a FilterStep above the view boundary and never reaches the view's inner
-- MergeTree reading step. The key then looks unused there even for a query with a valid partition /
-- primary-key predicate, so the guards threw incorrectly. The fix enables parallel_replicas_filter_pushdown
-- automatically when either guard is set, so the predicate is pushed through the view boundary into the
-- reading step: a covered predicate passes with parallel replicas still enabled, and a genuinely unused
-- index still throws (the negative cases below).
--
-- Known limitation: the pushed filter reaches a follower only through the shipped query AST. With
-- serialize_query_plan = 1 the initiator ships a query plan serialized before the pushdown, so a follower's
-- reading step never sees the predicate and the guards still throw a false-positive INDEX_NOT_USED.
-- Fixing that lane is deferred to plan-based parallel replicas, where index pushdown into a view works
-- automatically; serialize_query_plan must not be overridden by this fix (see the discussion in #109409).

DROP TABLE IF EXISTS t_force_index_pr;
DROP VIEW IF EXISTS v_force_index_pr;
DROP VIEW IF EXISTS vv_force_index_pr;
DROP VIEW IF EXISTS va_force_index_pr;

CREATE TABLE t_force_index_pr (timestamp DateTime, value UInt32)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY timestamp;

CREATE VIEW v_force_index_pr AS SELECT * FROM t_force_index_pr;
CREATE VIEW vv_force_index_pr AS SELECT * FROM v_force_index_pr;
CREATE VIEW va_force_index_pr AS SELECT timestamp AS ts, value AS val FROM t_force_index_pr;

INSERT INTO t_force_index_pr
SELECT toDateTime('2026-06-01 00:00:00') + number, number FROM numbers(1000000);

-- parallel_replicas_allow_view_over_mergetree = 0 pins the view-inner-query execution mode this PR fixes.
-- If that beta setting's default ever flips to 1, the view would be planned via the outer-view path and the
-- positive cases would pass without exercising the auto-enable logic, so lock it explicitly.
-- serialize_query_plan = 0 pins the AST transport for the shipped fragment: the pushed filter does not reach
-- a follower executing a serialized plan (the known limitation above), so the distributed-plan CI lane would
-- otherwise fail the positive cases.
SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_allow_view_over_mergetree = 0, serialize_query_plan = 0;

-- Base table with parallel replicas and a key predicate: the predicate folds into the reading step, so
-- this always worked. Kept as a control.
SELECT count() FROM t_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_index_by_date = 1, force_primary_key = 1, log_comment = '04501_fipr_base';

-- View with parallel replicas and a key predicate: this used to throw a false-positive INDEX_NOT_USED.
-- Now it returns the correct count. Exercise both guards together and each on its own.
SELECT count() FROM v_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_index_by_date = 1, force_primary_key = 1, log_comment = '04501_fipr_view_both';

SELECT count() FROM v_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_index_by_date = 1, log_comment = '04501_fipr_view_date';

SELECT count() FROM v_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_primary_key = 1, log_comment = '04501_fipr_view_pk';

-- View over a view, and a view with column aliases: the predicate must still reach the underlying table.
SELECT count() FROM vv_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_index_by_date = 1, force_primary_key = 1, log_comment = '04501_fipr_view_nested';

SELECT count() FROM va_force_index_pr
WHERE ts >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_index_by_date = 1, force_primary_key = 1, log_comment = '04501_fipr_view_alias';

-- The counts above match local execution, so they alone cannot prove parallel replicas stayed enabled:
-- a planner change silently falling back to local execution would keep this test green while re-breaking
-- the "parallel replicas stays enabled" part of the contract. Prove each positive actually used task-based
-- parallel replicas on the initiator, like 02950_parallel_replicas_used_count does.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment LIKE '04501\_fipr\_%'
    AND initial_query_id = query_id
ORDER BY log_comment
SETTINGS enable_parallel_replicas = 0;

-- View without parallel replicas with a key predicate: kept as a control.
SELECT count() FROM v_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS enable_parallel_replicas = 0, force_index_by_date = 1, force_primary_key = 1;

-- Negative: the contract must still hold. A query through the view whose predicate does not use the
-- primary key must still throw under parallel replicas (the guard is enforced at the reading step, not
-- silently turned into a no-op).
SELECT count() FROM v_force_index_pr
WHERE value = 1
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

-- Negative: same for force_index_by_date when the predicate does not use the partition key.
SELECT count() FROM v_force_index_pr
WHERE value = 1
SETTINGS force_index_by_date = 1; -- { serverError INDEX_NOT_USED }

-- Negative: contract holds through nested views too.
SELECT count() FROM vv_force_index_pr
WHERE value = 1
SETTINGS force_index_by_date = 1; -- { serverError INDEX_NOT_USED }

DROP VIEW va_force_index_pr;
DROP VIEW vv_force_index_pr;
DROP VIEW v_force_index_pr;
DROP TABLE t_force_index_pr;

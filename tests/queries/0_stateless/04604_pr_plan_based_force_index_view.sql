-- Tags: no-parallel-replicas
-- With plan-based parallel replicas, a query reading through a view pushes its WHERE predicate into the
-- view's inner MergeTree reading step automatically -- the planner builds a plain local plan and standard
-- filter push-down runs before the parallel-replicas split is inserted. So force_index_by_date /
-- force_primary_key over a view see the covered predicate and do NOT throw a false-positive
-- INDEX_NOT_USED, without enabling parallel_replicas_filter_pushdown (nor
-- parallel_replicas_allow_view_over_mergetree). A genuinely unused index still throws.
-- Compare with the non-plan-based / AST path, which needs the pushdown to be auto-enabled.
-- See issues #108266 / PR #109409.

DROP TABLE IF EXISTS t_force_index_pr_pb;
DROP VIEW IF EXISTS v_force_index_pr_pb;
DROP VIEW IF EXISTS vv_force_index_pr_pb;
DROP VIEW IF EXISTS va_force_index_pr_pb;

CREATE TABLE t_force_index_pr_pb (timestamp DateTime, value UInt32)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY timestamp;

CREATE VIEW v_force_index_pr_pb AS SELECT * FROM t_force_index_pr_pb;
CREATE VIEW vv_force_index_pr_pb AS SELECT * FROM v_force_index_pr_pb;
CREATE VIEW va_force_index_pr_pb AS SELECT timestamp AS ts, value AS val FROM t_force_index_pr_pb;

INSERT INTO t_force_index_pr_pb
SELECT toDateTime('2026-06-01 00:00:00') + number, number FROM numbers(1000000);

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 1;
-- Note: parallel_replicas_filter_pushdown and parallel_replicas_allow_view_over_mergetree are left at
-- their defaults (0): plan-based mode needs neither.

-- Base table with parallel replicas and a key predicate (always worked). Kept as a control.
SELECT count() FROM t_force_index_pr_pb
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_index_by_date = 1, force_primary_key = 1;

-- View with parallel replicas and a key predicate: the predicate must reach the underlying table.
-- Exercise both guards together and each on its own.
SELECT count() FROM v_force_index_pr_pb
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_index_by_date = 1, force_primary_key = 1;

SELECT count() FROM v_force_index_pr_pb
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_index_by_date = 1;

SELECT count() FROM v_force_index_pr_pb
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_primary_key = 1;

-- View over a view, and a view with column aliases: the predicate must still reach the underlying table.
SELECT count() FROM vv_force_index_pr_pb
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_index_by_date = 1, force_primary_key = 1;

SELECT count() FROM va_force_index_pr_pb
WHERE ts >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_index_by_date = 1, force_primary_key = 1;

-- View without parallel replicas with a key predicate. Kept as a control.
SELECT count() FROM v_force_index_pr_pb
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS enable_parallel_replicas = 0, force_index_by_date = 1, force_primary_key = 1;

-- Negative: a predicate that does not use the primary key must still throw under parallel replicas
-- (the guard is enforced at the reading step, not silently turned into a no-op).
SELECT count() FROM v_force_index_pr_pb
WHERE value = 1
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

-- Negative: same for force_index_by_date when the predicate does not use the partition key.
SELECT count() FROM v_force_index_pr_pb
WHERE value = 1
SETTINGS force_index_by_date = 1; -- { serverError INDEX_NOT_USED }

-- Negative: contract holds through nested views too.
SELECT count() FROM vv_force_index_pr_pb
WHERE value = 1
SETTINGS force_index_by_date = 1; -- { serverError INDEX_NOT_USED }

DROP VIEW va_force_index_pr_pb;
DROP VIEW vv_force_index_pr_pb;
DROP VIEW v_force_index_pr_pb;
DROP TABLE t_force_index_pr_pb;

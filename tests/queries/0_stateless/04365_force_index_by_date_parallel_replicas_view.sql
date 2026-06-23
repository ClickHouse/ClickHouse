-- Regression test for a false-positive INDEX_NOT_USED (error 277) thrown when a query reads through
-- a view with parallel replicas and force_index_by_date / force_primary_key enabled.
--
-- The query carries a valid partition/primary-key predicate, but under parallel replicas the reading
-- step that is analyzed does not see it when reading through a view: on the initiator the predicate
-- stays in a FilterStep above the view's subquery, and on a follower the shipped subquery carries no
-- predicate at all. The index then looks unused even though the query has a valid predicate, so the
-- guards used to throw incorrectly. See support-escalation #7976.

DROP TABLE IF EXISTS t_force_index_pr;
DROP VIEW IF EXISTS v_force_index_pr;

CREATE TABLE t_force_index_pr (timestamp DateTime)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY timestamp;

CREATE VIEW v_force_index_pr AS SELECT * FROM t_force_index_pr;

INSERT INTO t_force_index_pr
SELECT toDateTime('2026-06-01 00:00:00') + number FROM numbers(1000000);

-- Base table with parallel replicas: the predicate folds into the source step, so this always worked.
SELECT count() FROM t_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    force_index_by_date = 1, force_primary_key = 1;

-- View with parallel replicas: this used to throw a false-positive INDEX_NOT_USED on a follower.
SELECT count() FROM v_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    force_index_by_date = 1, force_primary_key = 1;

-- View without parallel replicas: the coordinator folds the predicate, so this always worked.
SELECT count() FROM v_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS enable_analyzer = 1, enable_parallel_replicas = 0,
    force_index_by_date = 1, force_primary_key = 1;

DROP VIEW v_force_index_pr;
DROP TABLE t_force_index_pr;

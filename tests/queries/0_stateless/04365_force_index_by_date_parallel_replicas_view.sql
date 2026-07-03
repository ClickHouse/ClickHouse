-- Regression test for a false-positive INDEX_NOT_USED (error 277) thrown when a query reads through a
-- view with parallel replicas and force_index_by_date / force_primary_key enabled. See issue #108266.
--
-- Under parallel replicas a view is executed as its inner subquery, so the outer WHERE predicate stays in
-- a FilterStep above the view boundary and never reaches the view's inner MergeTree reading step. The key
-- then looks unused there even for a query with a valid partition/primary-key predicate, so the guards
-- threw incorrectly. The fix reads the view without parallel replicas when either guard is set, so the
-- whole query is planned as one plan, the predicate is pushed into the reading step, and the guards are
-- enforced correctly: a covered predicate passes and a genuinely unused index still throws (the negative
-- cases below).

DROP TABLE IF EXISTS t_force_index_pr;
DROP VIEW IF EXISTS v_force_index_pr;

CREATE TABLE t_force_index_pr (timestamp DateTime, value UInt32)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY timestamp;

CREATE VIEW v_force_index_pr AS SELECT * FROM t_force_index_pr;

INSERT INTO t_force_index_pr
SELECT toDateTime('2026-06-01 00:00:00') + number, number FROM numbers(1000000);

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0;

-- Base table with parallel replicas and a key predicate: the predicate folds into the reading step, so
-- this always worked. Kept as a control.
SELECT count() FROM t_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_index_by_date = 1, force_primary_key = 1;

-- View with parallel replicas and a key predicate: this used to throw a false-positive INDEX_NOT_USED.
-- Now it returns the correct count. Exercise both guards together and each on its own.
SELECT count() FROM v_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_index_by_date = 1, force_primary_key = 1;

SELECT count() FROM v_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_index_by_date = 1;

SELECT count() FROM v_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS force_primary_key = 1;

-- View without parallel replicas with a key predicate: the coordinator folds the predicate, so this
-- always worked. Kept as a control.
SELECT count() FROM v_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS enable_parallel_replicas = 0, force_index_by_date = 1, force_primary_key = 1;

-- Negative: the contract must still hold. A query through the view whose predicate does not use the
-- primary key must still throw under parallel replicas (the guard is enforced on the complete plan, not
-- silently turned into a no-op).
SELECT count() FROM v_force_index_pr
WHERE value = 1
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

-- Negative: same for force_index_by_date when the predicate does not use the partition key.
SELECT count() FROM v_force_index_pr
WHERE value = 1
SETTINGS force_index_by_date = 1; -- { serverError INDEX_NOT_USED }

-- Same via the automatic parallel replicas heuristic (automatic_parallel_replicas_mode = 1). The heuristic
-- re-plans the query through the same view-inner path to build a parallel-replicas candidate, so the guards
-- must behave the same: a covered predicate passes and a genuinely unused index still throws. sum(value)
-- (instead of count()) is used so the heuristic actually builds the candidate plan for the view read;
-- min_bytes_per_replica = 1 stops it from skipping the candidate on size.
SELECT sum(value) FROM v_force_index_pr
WHERE timestamp >= toDateTime('2026-06-05 12:00:00')
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    automatic_parallel_replicas_min_bytes_per_replica = 1, force_index_by_date = 1, force_primary_key = 1;

SELECT sum(value) FROM v_force_index_pr
WHERE value = 1
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    automatic_parallel_replicas_min_bytes_per_replica = 1, force_primary_key = 1; -- { serverError INDEX_NOT_USED }

SELECT sum(value) FROM v_force_index_pr
WHERE value = 1
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    automatic_parallel_replicas_min_bytes_per_replica = 1, force_index_by_date = 1; -- { serverError INDEX_NOT_USED }

DROP VIEW v_force_index_pr;
DROP TABLE t_force_index_pr;

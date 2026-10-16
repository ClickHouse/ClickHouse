-- A pending `RENAME COLUMN` or `DROP COLUMN` of another column does not change the meaning of the
-- `num_defaults` stored for `x`, so `optimize_trivial_count_with_sparsity_filter` is still applied
-- to `count() WHERE x = 0`. It is not applied to the renamed column.

SET optimize_trivial_count_query = 1;
SET optimize_trivial_count_with_sparsity_filter = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_sparsity_pending_rename_other;
CREATE TABLE t_sparsity_pending_rename_other (x Int64, y Int64, z Int64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;
-- Only to hold the mutation pending deterministically.
SYSTEM STOP MERGES t_sparsity_pending_rename_other;
INSERT INTO t_sparsity_pending_rename_other VALUES (0, 100, 1), (0, 200, 2), (1, 0, 3);

ALTER TABLE t_sparsity_pending_rename_other RENAME COLUMN y TO w, DROP COLUMN z SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT count() FROM t_sparsity_pending_rename_other WHERE x = 0;
SELECT count() FROM t_sparsity_pending_rename_other WHERE w = 0;

SELECT countIf(explain LIKE '%Optimized trivial count with sparsity filter%') FROM (EXPLAIN SELECT count() FROM t_sparsity_pending_rename_other WHERE x = 0);
SELECT countIf(explain LIKE '%Optimized trivial count with sparsity filter%') FROM (EXPLAIN SELECT count() FROM t_sparsity_pending_rename_other WHERE w = 0);

SYSTEM START MERGES t_sparsity_pending_rename_other;
DROP TABLE t_sparsity_pending_rename_other;

-- `optimize_trivial_count_with_sparsity_filter` answers `count() WHERE x = 0` from the per-column
-- `num_defaults` stored in each part's `serialization.json`, which is keyed by the on-disk column
-- names. While a metadata mutation that changes column identity is still pending, reads already
-- apply it, so the stored counter of the on-disk `x` does not describe the values a query sees
-- under the name `x`: the rewrite must not be applied and the part has to be read instead.

SET optimize_trivial_count_query = 1;
SET optimize_trivial_count_with_sparsity_filter = 1;

-- A pending `RENAME COLUMN` onto the name of a dropped column: the on-disk `x` consists of
-- defaults only, while the column the query reads as `x` has no defaults.
DROP TABLE IF EXISTS t_sparsity_pending_rename;
CREATE TABLE t_sparsity_pending_rename (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;
-- Only to hold the mutation pending deterministically.
SYSTEM STOP MERGES t_sparsity_pending_rename;
INSERT INTO t_sparsity_pending_rename VALUES (0, 100), (0, 200);

ALTER TABLE t_sparsity_pending_rename DROP COLUMN x, RENAME COLUMN y TO x SETTINGS alter_sync = 0, mutations_sync = 0;

-- The rename is already applied to reads.
SELECT groupArray(x) FROM t_sparsity_pending_rename;
SELECT count(), (SELECT count() FROM t_sparsity_pending_rename WHERE x = 0 SETTINGS optimize_trivial_count_with_sparsity_filter = 0) FROM t_sparsity_pending_rename WHERE x = 0;
SELECT count(), (SELECT count() FROM t_sparsity_pending_rename WHERE x != 0 SETTINGS optimize_trivial_count_with_sparsity_filter = 0) FROM t_sparsity_pending_rename WHERE x != 0;

SYSTEM START MERGES t_sparsity_pending_rename;
DROP TABLE t_sparsity_pending_rename;

-- A pending `DROP COLUMN` followed by re-adding a column with the same name: reads treat the
-- on-disk data as missing and fill the default, while the stored counter still describes the
-- dropped column's data, which had no defaults.
DROP TABLE IF EXISTS t_sparsity_pending_readd;
CREATE TABLE t_sparsity_pending_readd (id Int64, x Int64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;
SYSTEM STOP MERGES t_sparsity_pending_readd;
INSERT INTO t_sparsity_pending_readd VALUES (1, 1), (2, 2);

ALTER TABLE t_sparsity_pending_readd DROP COLUMN x, ADD COLUMN x Int64 SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT groupArray(x) FROM t_sparsity_pending_readd;
SELECT count(), (SELECT count() FROM t_sparsity_pending_readd WHERE x = 0 SETTINGS optimize_trivial_count_with_sparsity_filter = 0) FROM t_sparsity_pending_readd WHERE x = 0;
SELECT count(), (SELECT count() FROM t_sparsity_pending_readd WHERE x != 0 SETTINGS optimize_trivial_count_with_sparsity_filter = 0) FROM t_sparsity_pending_readd WHERE x != 0;

SYSTEM START MERGES t_sparsity_pending_readd;
DROP TABLE t_sparsity_pending_readd;

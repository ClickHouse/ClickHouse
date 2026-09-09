-- Tags: no-parallel-replicas, no-replicated-database
-- The trivial count with sparsity filter rewrite serves `SELECT count() FROM t WHERE <pred>`
-- from the per-column `num_defaults` recorded in `serialization.json`. A pending metadata
-- mutation changes what the column's values (or its name) mean without rewriting the part:
-- reads already apply the change on the fly, while the recorded `num_defaults` still describes
-- the original data, so the rewrite would return a silently wrong count. Defaultness stats must
-- fail open on metadata mutations the same way they do on alter mutations.
--
-- Covered: DROP + re-ADD of the same name, and DROP + RENAME into the freed name.

SET optimize_trivial_count_with_sparsity_filter = 1;
SET enable_analyzer = 1;

-- Pending DROP COLUMN + ADD COLUMN with a DEFAULT: reads return the re-added column's default
-- for every row, but the recorded num_defaults still counts the original values.
DROP TABLE IF EXISTS num_defaults_readded;
CREATE TABLE num_defaults_readded (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS compute_exact_num_defaults_for_sparse_columns = 1;
SYSTEM STOP MERGES num_defaults_readded;
INSERT INTO num_defaults_readded VALUES (1, 100), (2, 200);
ALTER TABLE num_defaults_readded (DROP COLUMN x), (ADD COLUMN x UInt8 DEFAULT 0)
    SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT 'drop+add reads', groupArray(x) FROM num_defaults_readded;
SELECT 'drop+add x = 0', count() FROM num_defaults_readded WHERE x = 0;
SELECT 'drop+add x != 0', count() FROM num_defaults_readded WHERE x != 0;
DROP TABLE num_defaults_readded;

-- Pending DROP COLUMN + RENAME COLUMN into the freed name: reads return the renamed column's
-- data, but the recorded num_defaults still counts the dropped column's values.
DROP TABLE IF EXISTS num_defaults_renamed;
CREATE TABLE num_defaults_renamed (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS compute_exact_num_defaults_for_sparse_columns = 1;
SYSTEM STOP MERGES num_defaults_renamed;
INSERT INTO num_defaults_renamed VALUES (1, 0), (2, 0);
ALTER TABLE num_defaults_renamed (DROP COLUMN x), (RENAME COLUMN y TO x)
    SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT 'drop+rename reads', groupArray(x) FROM num_defaults_renamed;
SELECT 'drop+rename x = 0', count() FROM num_defaults_renamed WHERE x = 0;
DROP TABLE num_defaults_renamed;

-- Tags: no-ordinary-database, no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- UNIQUE KEY: indirect MATERIALIZE COLUMN guard.
-- See https://github.com/ClickHouse/ClickHouse/pull/99647
--
-- `MergeTreeData::checkMutationIsPossible` rejects MATERIALIZE COLUMN of a *direct* UNIQUE KEY
-- column, but it cannot see that a *dependent* stored MATERIALIZED column is part of the UNIQUE KEY.
-- `MATERIALIZE COLUMN c2`, where `c2` feeds a stored MATERIALIZED column `k` that is in the UNIQUE
-- KEY, would recompute `k` in the dependent stage and rewrite its stored values outside the UNIQUE
-- KEY dedup path, which could produce duplicate live keys. `MutationsInterpreter::prepare` refuses it.

SET allow_experimental_unique_key = 1;

DROP TABLE IF EXISTS uk_mat_indirect;

-- `k` is a stored MATERIALIZED column computed from `c2` and is part of the UNIQUE KEY.
CREATE TABLE uk_mat_indirect (a UInt32, c2 UInt32 MATERIALIZED a + 1, k UInt32 MATERIALIZED c2 + 1)
ENGINE = MergeTree ORDER BY a UNIQUE KEY (k);

-- Materializing `c2` would recompute the dependent UNIQUE KEY column `k`: refused.
ALTER TABLE uk_mat_indirect MATERIALIZE COLUMN c2; -- { serverError CANNOT_UPDATE_COLUMN }

DROP TABLE uk_mat_indirect;

-- Negative: a dependent MATERIALIZED column that is NOT part of the UNIQUE KEY can be recomputed,
-- so the mutation must be accepted.
DROP TABLE IF EXISTS uk_mat_indirect_ok;
CREATE TABLE uk_mat_indirect_ok (a UInt32, c2 UInt32 MATERIALIZED a + 1, m UInt32 MATERIALIZED c2 + 1)
ENGINE = MergeTree ORDER BY a UNIQUE KEY (a);
ALTER TABLE uk_mat_indirect_ok MATERIALIZE COLUMN c2;
SELECT 'ok';
DROP TABLE uk_mat_indirect_ok;

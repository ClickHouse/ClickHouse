-- Tags: no-ordinary-database, no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- UNIQUE KEY: indirect and subcolumn MATERIALIZE COLUMN guard.
-- See https://github.com/ClickHouse/ClickHouse/pull/99647
--
-- `MergeTreeData::checkMutationIsPossible` rejects MATERIALIZE COLUMN of a UNIQUE KEY column, but only
-- by an exact name match. So it misses two cases that `MutationsInterpreter::prepare` must refuse:
--   1. A *dependent* stored MATERIALIZED column is part of the UNIQUE KEY. `MATERIALIZE COLUMN c2`,
--      where `c2` feeds a stored MATERIALIZED column `k` that is in the UNIQUE KEY, would recompute
--      `k` in the dependent stage and rewrite its stored values outside the UNIQUE KEY dedup path.
--   2. The target is the *parent* of a UNIQUE KEY subcolumn. `UNIQUE KEY (tup.k)` is accepted, but
--      `MATERIALIZE COLUMN tup` rewrites the whole Tuple — including the stored `tup.k` — without
--      going through the UNIQUE KEY dedup path.
-- Either could produce duplicate live keys, so both are refused with `CANNOT_UPDATE_COLUMN`.

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

-- A Tuple subcolumn `tup.k` is accepted as a UNIQUE KEY column. `checkMutationIsPossible` only does an
-- exact-name check (`tup` != `tup.k`), so `MATERIALIZE COLUMN tup` slips past it; `prepare` must refuse
-- the parent because rewriting `tup` rewrites the stored `tup.k`.
DROP TABLE IF EXISTS uk_mat_subcolumn;
CREATE TABLE uk_mat_subcolumn (a UInt32, tup Tuple(k UInt32, v UInt32) MATERIALIZED (a + 1, a + 2))
ENGINE = MergeTree ORDER BY a UNIQUE KEY (tup.k);
ALTER TABLE uk_mat_subcolumn MATERIALIZE COLUMN tup; -- { serverError CANNOT_UPDATE_COLUMN }
DROP TABLE uk_mat_subcolumn;

-- Negative: a Tuple whose subcolumn is NOT part of the UNIQUE KEY can be materialized.
DROP TABLE IF EXISTS uk_mat_subcolumn_ok;
CREATE TABLE uk_mat_subcolumn_ok (a UInt32, tup Tuple(k UInt32, v UInt32) MATERIALIZED (a + 1, a + 2))
ENGINE = MergeTree ORDER BY a UNIQUE KEY (a);
ALTER TABLE uk_mat_subcolumn_ok MATERIALIZE COLUMN tup;
SELECT 'ok';
DROP TABLE uk_mat_subcolumn_ok;

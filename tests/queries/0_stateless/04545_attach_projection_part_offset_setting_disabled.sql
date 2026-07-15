-- Tags: no-parallel-replicas

-- Regression for issue #102445: a table with a projection using a gated virtual column must
-- remain attachable after the CREATE-time feature gate setting is disabled. Those gates in
-- checkProperties must be skipped on ATTACH, otherwise the table becomes permanently
-- unattachable after DETACH / server restart.
--
-- Only the pure CREATE-time gates may be skipped on ATTACH:
--   allow_part_offset_column_in_projections, allow_commit_order_projection (read only at CREATE).
-- The block-number / block-offset gates are NOT CREATE-only: a commit-order projection can be
-- rebuilt from the base part during a merge, and that rebuild produces _block_number /
-- _block_offset only when enable_block_number_column / enable_block_offset_column are enabled
-- (MergeTask::enabledBlockNumberColumn / enabledBlockOffsetColumn). Attaching with them disabled
-- would let a later merge run without the columns the projection requires, so they stay validated
-- on ATTACH.

-- (1) _part_offset projection: allow_part_offset_column_in_projections is CREATE-only -> ATTACH must succeed.
DROP TABLE IF EXISTS t_04545_po;
CREATE TABLE t_04545_po (a UInt64, b UInt64,
    PROJECTION p (SELECT a, b, _part_offset ORDER BY b))
ENGINE = MergeTree ORDER BY a
SETTINGS allow_part_offset_column_in_projections = 1;
INSERT INTO t_04545_po VALUES (1, 1), (2, 2);
ALTER TABLE t_04545_po MODIFY SETTING allow_part_offset_column_in_projections = 0;
DETACH TABLE t_04545_po;
ATTACH TABLE t_04545_po;
SELECT '_part_offset attach', count() FROM t_04545_po;
DROP TABLE t_04545_po;

-- (2) commit-order projection: allow_commit_order_projection is CREATE-only -> ATTACH must succeed.
DROP TABLE IF EXISTS t_04545_co;
CREATE TABLE t_04545_co (a UInt64,
    PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_04545_co(a) VALUES (1), (2);
ALTER TABLE t_04545_co MODIFY SETTING allow_commit_order_projection = 0;
DETACH TABLE t_04545_co;
ATTACH TABLE t_04545_co;
SELECT 'commit_order attach', count() FROM t_04545_co;
DROP TABLE t_04545_co;

-- enable_block_number_column / enable_block_offset_column are merge-time dependencies of a
-- commit-order projection, so they must be validated against the EFFECTIVE post-ALTER settings,
-- not the live ones. Disabling them via ALTER while such a projection exists must be rejected up
-- front (otherwise the ALTER is accepted but a later merge / MATERIALIZE PROJECTION rebuild runs
-- without materializing the required _block_number / _block_offset). checkProperties reconstructs
-- the effective settings the same way the real settings-update path does: getDefaultSettings()
-- overlaid with new_metadata.settings_changes. That base matters for RESET SETTING (cases 7/8):
-- RESET drops the key from settings_changes, so the effective value must fall back to the DEFAULT
-- (0 here), not to the old live value.

-- (3) ALTER disabling enable_block_number_column while a commit-order projection exists -> rejected.
DROP TABLE IF EXISTS t_04545_bn;
CREATE TABLE t_04545_bn (a UInt64,
    PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_04545_bn(a) VALUES (1), (2);
ALTER TABLE t_04545_bn MODIFY SETTING enable_block_number_column = 0; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_04545_bn;

-- (4) ALTER disabling enable_block_offset_column while a commit-order projection exists -> rejected.
DROP TABLE IF EXISTS t_04545_bo;
CREATE TABLE t_04545_bo (a UInt64,
    PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_04545_bo(a) VALUES (1), (2);
ALTER TABLE t_04545_bo MODIFY SETTING enable_block_offset_column = 0; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_04545_bo;

-- (5) control: disabling enable_block_number_column on a table WITHOUT a commit-order projection
-- must stay allowed (nothing depends on the column).
DROP TABLE IF EXISTS t_04545_plain;
CREATE TABLE t_04545_plain (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1;
ALTER TABLE t_04545_plain MODIFY SETTING enable_block_number_column = 0;
SELECT 'plain disable ok';
DROP TABLE t_04545_plain;

-- (6) control: disabling the CREATE-only allow_commit_order_projection via ALTER stays allowed
-- (issue #102445 principle: it is not consulted after CREATE).
DROP TABLE IF EXISTS t_04545_coa;
CREATE TABLE t_04545_coa (a UInt64,
    PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_04545_coa(a) VALUES (1), (2);
ALTER TABLE t_04545_coa MODIFY SETTING allow_commit_order_projection = 0;
SELECT 'commit_order create-only disable ok';
DROP TABLE t_04545_coa;

-- (7) RESET SETTING enable_block_number_column while a commit-order projection exists -> rejected.
-- RESET drops the key from settings_changes, so the effective value falls back to the DEFAULT (0);
-- validating against getDefaultSettings() + settings_changes catches it (validating against the
-- live settings overlaid with settings_changes would wrongly keep the old value = 1).
DROP TABLE IF EXISTS t_04545_rbn;
CREATE TABLE t_04545_rbn (a UInt64,
    PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_04545_rbn(a) VALUES (1), (2);
ALTER TABLE t_04545_rbn RESET SETTING enable_block_number_column; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_04545_rbn;

-- (8) RESET SETTING enable_block_offset_column while a commit-order projection exists -> rejected.
DROP TABLE IF EXISTS t_04545_rbo;
CREATE TABLE t_04545_rbo (a UInt64,
    PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_04545_rbo(a) VALUES (1), (2);
ALTER TABLE t_04545_rbo RESET SETTING enable_block_offset_column; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_04545_rbo;

-- allow_part_offset_column_in_projections / allow_commit_order_projection are pure CREATE-time
-- gates: nothing at merge / MATERIALIZE PROJECTION time reads them. They must fire ONLY for a
-- projection introduced by the current operation (CREATE / ADD PROJECTION), validated against the
-- effective post-operation settings, and never be re-applied to a pre-existing projection on an
-- unrelated later ALTER.

-- (9) after disabling allow_commit_order_projection on a table that already has such a projection,
-- an unrelated ALTER (ADD COLUMN) must NOT be rejected -- no new projection is being created.
DROP TABLE IF EXISTS t_04545_unrel;
CREATE TABLE t_04545_unrel (a UInt64,
    PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_04545_unrel(a) VALUES (1), (2);
ALTER TABLE t_04545_unrel MODIFY SETTING allow_commit_order_projection = 0;
ALTER TABLE t_04545_unrel ADD COLUMN c UInt64;
SELECT 'unrelated alter after disable ok';
DROP TABLE t_04545_unrel;

-- (10) a mixed ALTER that both ADDs a commit-order projection and enables the gate must succeed:
-- the gate is validated against the effective post-ALTER settings, not the stale live value.
DROP TABLE IF EXISTS t_04545_mix;
CREATE TABLE t_04545_mix (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
ALTER TABLE t_04545_mix
    ADD PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset),
    MODIFY SETTING allow_commit_order_projection = 1;
SELECT 'mixed add-projection enable-gate ok';
DROP TABLE t_04545_mix;

-- (11) control: ADD PROJECTION introducing a commit-order projection while the gate is still off
-- must be rejected (the gate still fires for a projection introduced by the current operation).
DROP TABLE IF EXISTS t_04545_addoff;
CREATE TABLE t_04545_addoff (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 0;
ALTER TABLE t_04545_addoff ADD PROJECTION p (SELECT a, _block_number ORDER BY _block_number); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_04545_addoff;

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

-- (3) commit-order projection: enable_block_number_column is a merge-time dependency ->
-- ATTACH must still be rejected when it is disabled (would otherwise attach into a state where
-- the next commit-order projection rebuild runs without _block_number). Left detached; the
-- per-test database drop cleans it up.
DROP TABLE IF EXISTS t_04545_bn;
CREATE TABLE t_04545_bn (a UInt64,
    PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_04545_bn(a) VALUES (1), (2);
ALTER TABLE t_04545_bn MODIFY SETTING enable_block_number_column = 0;
DETACH TABLE t_04545_bn;
ATTACH TABLE t_04545_bn; -- { serverError BAD_ARGUMENTS }

-- (4) commit-order projection: enable_block_offset_column is also a merge-time dependency ->
-- ATTACH must still be rejected when it is disabled.
DROP TABLE IF EXISTS t_04545_bo;
CREATE TABLE t_04545_bo (a UInt64,
    PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_04545_bo(a) VALUES (1), (2);
ALTER TABLE t_04545_bo MODIFY SETTING enable_block_offset_column = 0;
DETACH TABLE t_04545_bo;
ATTACH TABLE t_04545_bo; -- { serverError BAD_ARGUMENTS }

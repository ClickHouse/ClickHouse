-- Verify that CLEAR COLUMN on a table with a projection does not cause
-- a logical error during OPTIMIZE FINAL on compact parts.
-- Regression test for a bug where projection parts (which have a fake
-- data_version of 0) were incorrectly treated as having pending DROP_COLUMN
-- mutations, causing the compact reader to partially read Array columns
-- (offsets only, values filled with defaults) and violating sort order.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_clear_col_proj;

CREATE TABLE t_clear_col_proj
(
    c0 Int,
    c1 Array(String),
    PROJECTION p0 (SELECT * ORDER BY c1)
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO t_clear_col_proj SELECT 0, ['a'];
ALTER TABLE t_clear_col_proj CLEAR COLUMN c1;
INSERT INTO t_clear_col_proj SELECT 0, ['a', 'b'] UNION ALL SELECT 0, ['c'];
OPTIMIZE TABLE t_clear_col_proj FINAL;

SELECT * FROM t_clear_col_proj ORDER BY c1;

DROP TABLE t_clear_col_proj;

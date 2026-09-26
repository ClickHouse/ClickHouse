-- A column added by `ALTER TABLE ... ADD COLUMN` is missing from the blocks written before the
-- `ALTER`. The reading source reads it as the default value of its type, so `PREWHERE` on a column
-- with a `DEFAULT` expression is rejected, while `PREWHERE` on a column without one is allowed and
-- gives the same result as `WHERE`.

SET optimize_move_to_prewhere = 1;

DROP TABLE IF EXISTS t_memory_added_column;

CREATE TABLE t_memory_added_column (k UInt64) ENGINE = Memory;
INSERT INTO t_memory_added_column VALUES (1), (2), (3);

ALTER TABLE t_memory_added_column ADD COLUMN d UInt64 DEFAULT k * 10;
ALTER TABLE t_memory_added_column ADD COLUMN e UInt64;
INSERT INTO t_memory_added_column (k) VALUES (4);

SELECT k, d, e FROM t_memory_added_column ORDER BY k;

-- `d` has a `DEFAULT` expression, so it is not a part of the `PREWHERE` contract.
SELECT k FROM t_memory_added_column PREWHERE d = 40; -- { serverError ILLEGAL_PREWHERE }
-- The `WHERE` on it still works and is not moved to `PREWHERE`.
SELECT k FROM t_memory_added_column WHERE d = 40 ORDER BY k;

-- `e` has no `DEFAULT` expression: `PREWHERE` and `WHERE` see the same values.
SELECT k FROM t_memory_added_column PREWHERE e = 0 ORDER BY k;
SELECT k FROM t_memory_added_column WHERE e = 0 ORDER BY k SETTINGS optimize_move_to_prewhere = 0;

-- An `ALIAS` column is never stored, and `PREWHERE` on it is rejected as well.
ALTER TABLE t_memory_added_column ADD COLUMN a UInt64 ALIAS k * 2;
SELECT k FROM t_memory_added_column PREWHERE a = 4; -- { serverError ILLEGAL_PREWHERE }

DROP TABLE t_memory_added_column;

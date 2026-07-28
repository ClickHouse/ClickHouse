-- Regression test: a `set` skip index must not pull columns it does not index
-- into its per-granule filter expression.
--
-- When a row policy `USING` expression contains a constant-true term expressed as a
-- function (e.g. `arrayExists(x -> 1 = 1, [...])`) OR-ed with a term over a non-indexed
-- column, the analyzer folds the OR to a constant value but keeps the call tree (with
-- the non-indexed column) underneath. The set-index condition used to copy that whole
-- subtree into its `actions`, then fail at read time with NOT_FOUND_COLUMN_IN_BLOCK
-- because the index granule block only carries the indexed column.

DROP ROW POLICY IF EXISTS rp_04340 ON t_04340;
DROP TABLE IF EXISTS t_04340;

CREATE TABLE t_04340
(
    a UInt32,
    b UInt32,
    INDEX ia a TYPE set(0) GRANULARITY 4
)
ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 4;

INSERT INTO t_04340 VALUES (1, 10), (2, 20), (3, 30), (123, 40);

-- Always-true policy (the higher-order const-true term short-circuits the OR), so every
-- row is visible. The non-indexed column `b` only appears inside the OR-ed-away term.
CREATE ROW POLICY rp_04340 ON t_04340 FOR SELECT
USING arrayExists(x -> (1 = 1), ['y']) OR has([], b)
TO ALL;

-- These used to throw `Code: 10 NOT_FOUND_COLUMN_IN_BLOCK: Not found column b`.
SELECT count() FROM t_04340 WHERE a = 123;
SELECT * FROM t_04340 ORDER BY a;

-- Same trigger with the higher-order shape (lambda capturing the column).
DROP ROW POLICY rp_04340 ON t_04340;
CREATE ROW POLICY rp_04340 ON t_04340 FOR SELECT
USING arrayExists(x -> (1 = 1), ['y']) OR arrayExists(x -> has([], b), ['y'])
TO ALL;

SELECT count() FROM t_04340 WHERE a = 123;

DROP ROW POLICY rp_04340 ON t_04340;
DROP TABLE t_04340;

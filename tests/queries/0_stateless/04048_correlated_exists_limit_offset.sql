-- Bug test for PR #99005 (Fix EXISTS not respecting LIMIT and OFFSET in subqueries).
-- PR #99005 correctly fixes non-correlated EXISTS with LIMIT/OFFSET.
-- Correlated EXISTS with LIMIT/OFFSET is now supported via LimitStep decorrelation
-- (LimitStep is converted to LimitByStep grouped by correlated columns).
-- Requires the new analyzer for correlated subquery decorrelation.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t_04048_outer;
DROP TABLE IF EXISTS t_04048_inner;

CREATE TABLE t_04048_outer (x UInt64) ENGINE = MergeTree() ORDER BY x;
CREATE TABLE t_04048_inner (x UInt64) ENGINE = MergeTree() ORDER BY x;

INSERT INTO t_04048_outer VALUES (1), (2), (3);
INSERT INTO t_04048_inner VALUES (1), (1), (1), (2), (2);

-- Correlated EXISTS with OFFSET that skips all matching rows → false
SELECT x, EXISTS (
    SELECT 1 FROM t_04048_inner
    WHERE t_04048_inner.x = t_04048_outer.x
    LIMIT 3 OFFSET 10
) AS e FROM t_04048_outer ORDER BY x;

-- Correlated EXISTS with LIMIT 0 → false
SELECT x, EXISTS (
    SELECT 1 FROM t_04048_inner
    WHERE t_04048_inner.x = t_04048_outer.x
    LIMIT 0
) AS e FROM t_04048_outer ORDER BY x;


DROP TABLE t_04048_outer;
DROP TABLE t_04048_inner;

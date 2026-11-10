-- Bug test for PR #99005 (Fix EXISTS not respecting LIMIT and OFFSET in subqueries).
-- PR #99005 correctly fixes non-correlated EXISTS with LIMIT/OFFSET, but correlated
-- EXISTS with LIMIT offset>0 or limit=0 now throws NOT_IMPLEMENTED because
-- optimizePlanForExists preserves the LimitStep while decorrelateQueryPlan cannot
-- handle it.

DROP TABLE IF EXISTS t_04048_outer;
DROP TABLE IF EXISTS t_04048_inner;

CREATE TABLE t_04048_outer (x UInt64) ENGINE = MergeTree() ORDER BY x;
CREATE TABLE t_04048_inner (x UInt64) ENGINE = MergeTree() ORDER BY x;

INSERT INTO t_04048_outer VALUES (1), (2), (3);
INSERT INTO t_04048_inner VALUES (1), (1), (1), (2), (2);

-- Correlated EXISTS with OFFSET that skips all matching rows → NOT_IMPLEMENTED
SELECT x, EXISTS (
    SELECT 1 FROM t_04048_inner
    WHERE t_04048_inner.x = t_04048_outer.x
    LIMIT 3 OFFSET 10
) AS e FROM t_04048_outer ORDER BY x; -- { serverError NOT_IMPLEMENTED, UNKNOWN_IDENTIFIER }

-- Correlated EXISTS with LIMIT 0 → NOT_IMPLEMENTED
SELECT x, EXISTS (
    SELECT 1 FROM t_04048_inner
    WHERE t_04048_inner.x = t_04048_outer.x
    LIMIT 0
) AS e FROM t_04048_outer ORDER BY x; -- { serverError NOT_IMPLEMENTED, UNKNOWN_IDENTIFIER }


DROP TABLE t_04048_outer;
DROP TABLE t_04048_inner;

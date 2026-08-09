-- Follow-up review test for INSERT into regular views (issue #91535).
--
-- A regular view whose FROM clause references another regular view (a nested view chain) must
-- not be insertable. The intermediate view carries no column `DEFAULT`s, so an omitted column
-- would be materialized as a type default and the final storage's `DEFAULT` expression would never
-- apply: `INSERT INTO v2 (a)` below would otherwise store `b = 0` instead of `t.b`'s DEFAULT 42.
-- Such chains are rejected with `NOT_IMPLEMENTED`.

DROP VIEW IF EXISTS v_nested_outer;
DROP VIEW IF EXISTS v_nested_inner;
DROP TABLE IF EXISTS t_nested;

CREATE TABLE t_nested (a UInt8, b UInt8 DEFAULT 42) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v_nested_inner AS SELECT * FROM t_nested;
CREATE VIEW v_nested_outer AS SELECT * FROM v_nested_inner;

-- A direct view over the real table is insertable and applies the target DEFAULT.
INSERT INTO v_nested_inner (a) VALUES (1);
SELECT 'direct:', a, b FROM t_nested ORDER BY a;

-- A view whose target is itself a view is rejected.
INSERT INTO v_nested_outer (a) VALUES (2); -- { serverError NOT_IMPLEMENTED }

-- The rejection holds for a full column list too.
INSERT INTO v_nested_outer (a, b) VALUES (2, 7); -- { serverError NOT_IMPLEMENTED }

SELECT 'after_rejected:', a, b FROM t_nested ORDER BY a;

DROP VIEW v_nested_outer;
DROP VIEW v_nested_inner;
DROP TABLE t_nested;

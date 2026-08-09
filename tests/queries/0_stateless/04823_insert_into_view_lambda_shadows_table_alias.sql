-- A lambda parameter that shadows the table alias must not be rewritten by the insert-time
-- constraint check: in `FROM t AS x WHERE arrayExists(x -> x.a > 0, arr)` the compound `x.a`
-- is the tuple element `a` of the lambda parameter, not the table-qualified column `a`.
-- Such a constraint cannot be evaluated by the write path, so the insert must fail close
-- with `NOT_IMPLEMENTED` instead of silently enforcing the wrong predicate.

DROP TABLE IF EXISTS t_lambda_shadow;
DROP TABLE IF EXISTS v_lambda_shadow;
DROP TABLE IF EXISTS v_lambda_plain;
DROP TABLE IF EXISTS v_qualified;

CREATE TABLE t_lambda_shadow (a Int32, arr Array(Tuple(a Int32)), nums Array(Int32)) ENGINE = MergeTree ORDER BY tuple();

CREATE VIEW v_lambda_shadow AS SELECT a, arr, nums FROM t_lambda_shadow AS x WHERE arrayExists(x -> x.a > 0, arr);

-- Before the fix, `x.a` was rewritten to the outer column `a`, so this insert (outer `a` = 0,
-- tuple field `a` = 1) was rejected and the next one (outer `a` = 1, tuple field `a` = 0) was
-- accepted — the exact opposite of the view's read-time WHERE. Both must fail close instead.
INSERT INTO v_lambda_shadow VALUES (0, [(1)], []); -- { serverError NOT_IMPLEMENTED }
INSERT INTO v_lambda_shadow VALUES (1, [(0)], []); -- { serverError NOT_IMPLEMENTED }

SELECT count() FROM t_lambda_shadow;

-- A lambda parameter shadowing the table alias without compound access stays insertable.
CREATE VIEW v_lambda_plain AS SELECT a, arr, nums FROM t_lambda_shadow AS x WHERE arrayExists(x -> x > 0, nums);

INSERT INTO v_lambda_plain VALUES (1, [], [1]);
INSERT INTO v_lambda_plain VALUES (2, [], [0]); -- { serverError VIOLATED_CONSTRAINT }

-- A table-qualified reference outside any lambda is still resolved against the target table.
CREATE VIEW v_qualified AS SELECT a, arr, nums FROM t_lambda_shadow AS x WHERE x.a > 10;

INSERT INTO v_qualified VALUES (11, [(1)], []);
INSERT INTO v_qualified VALUES (5, [(1)], []); -- { serverError VIOLATED_CONSTRAINT }

SELECT a, arr, nums FROM t_lambda_shadow ORDER BY a;

DROP TABLE v_qualified;
DROP TABLE v_lambda_plain;
DROP TABLE v_lambda_shadow;
DROP TABLE t_lambda_shadow;

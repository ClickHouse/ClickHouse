-- Cycle detection for DEFAULT expressions must be lambda-scope-aware: a lambda
-- argument whose name matches a column must not be treated as a dependency on
-- that column, so no false CYCLIC_ALIASES exception is raised.

DROP TABLE IF EXISTS t_default_lambda_arg;

CREATE TABLE t_default_lambda_arg
(
    a UInt64,
    x UInt64 DEFAULT a + 1,
    d Array(UInt64) DEFAULT arrayMap(x -> x + 1, [a])
)
ENGINE = MergeTree ORDER BY a;

-- The existing default of `d` uses a lambda argument named `x`. Altering the
-- column `x` must not report a cycle `x -> d -> x`: the `x` inside the lambda
-- body is the lambda argument, not the column.
ALTER TABLE t_default_lambda_arg MODIFY COLUMN x UInt64 DEFAULT a + 2;

INSERT INTO t_default_lambda_arg (a) VALUES (10);
SELECT a, x, d FROM t_default_lambda_arg ORDER BY a;

-- The same holds at CREATE time and when the altered default itself contains
-- such a lambda.
ALTER TABLE t_default_lambda_arg MODIFY COLUMN d Array(UInt64) DEFAULT arrayMap(x -> x * 2, [a]);

INSERT INTO t_default_lambda_arg (a) VALUES (20);
SELECT a, x, d FROM t_default_lambda_arg ORDER BY a;

-- A genuine cycle (through column references outside a lambda scope) is still detected.
ALTER TABLE t_default_lambda_arg MODIFY COLUMN d Array(UInt64) DEFAULT arrayMap(y -> y + x, [a]);
ALTER TABLE t_default_lambda_arg MODIFY COLUMN x UInt64 DEFAULT length(d); -- { serverError CYCLIC_ALIASES }

DROP TABLE t_default_lambda_arg;

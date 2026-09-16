-- `REPLACE` must substitute the pre-`REPLACE` expression only for free references to the
-- replaced column inside the replacement expression: identifiers bound by an enclosing lambda
-- parameter of the same name are different variables, and lambda parameter lists must never
-- be rewritten.

DROP TABLE IF EXISTS t_replace_lambda_scope;
CREATE TABLE t_replace_lambda_scope
(
    a UInt64,
    m UInt64 MATERIALIZED arrayElement(array(COLUMNS('^a$') REPLACE (a + 10 AS a) REPLACE (arrayMap(a -> a + 1, [a]) AS a) APPLY (x -> arrayElement(x, 1))), 1)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_replace_lambda_scope (a) VALUES (5);

-- m: [a] -> [a + 10] -> arrayMap(a -> a + 1, [15]) = [16]
SELECT a, m FROM t_replace_lambda_scope;

DROP TABLE t_replace_lambda_scope;

-- A lambda body reference bound by the lambda parameter must survive even when the lambda is
-- applied to elements other than the replaced column.
DROP TABLE IF EXISTS t_replace_lambda_scope_sum;
CREATE TABLE t_replace_lambda_scope_sum
(
    a UInt64,
    s UInt64 MATERIALIZED arrayElement(array(COLUMNS('^a$') REPLACE (a + 10 AS a) REPLACE (arraySum(arrayMap(a -> a + 100, [a, 1])) AS a)), 1)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_replace_lambda_scope_sum (a) VALUES (5);

-- s: arraySum(arrayMap(a -> a + 100, [15, 1])) = 115 + 101 = 216
SELECT a, s FROM t_replace_lambda_scope_sum;

DROP TABLE t_replace_lambda_scope_sum;

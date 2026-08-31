-- https://github.com/ClickHouse/ClickHouse/issues/117016
-- The plan-time join conversions decide by evaluating the filter over fabricated one-row inputs, and
-- guard against a non-deterministic filter. A non-deterministic call that depends on a lambda
-- argument stays inside the lambda's captured `ActionsDAG`, where the guard could not see it, so a
-- single plan-time draw decided whether `LEFT` became `INNER` (or `ANY LEFT` became `SEMI`) and the
-- resulting plan dropped every not-matched row.

DROP TABLE IF EXISTS t_join_lambda_left;
DROP TABLE IF EXISTS t_join_lambda_right;
CREATE TABLE t_join_lambda_left (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_join_lambda_right (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_join_lambda_left SELECT number FROM numbers(1000);
INSERT INTO t_join_lambda_right SELECT number, number FROM numbers(10);

-- The plan must keep the join kind and strictness, so the count stays binomial around 500 rather
-- than collapsing to the ~5 matched rows.
SELECT count() BETWEEN 400 AND 600
FROM t_join_lambda_left ANY LEFT JOIN t_join_lambda_right ON t_join_lambda_left.k = t_join_lambda_right.k
WHERE t_join_lambda_right.v = 12345 OR arrayExists(x -> rand(x) % 2 = 0, materialize([1]));

SELECT count() BETWEEN 400 AND 600
FROM t_join_lambda_left LEFT JOIN t_join_lambda_right ON t_join_lambda_left.k = t_join_lambda_right.k
WHERE t_join_lambda_right.v = 12345 OR arrayExists(x -> rand(x) % 2 = 0, materialize([1]));

SELECT count() FROM (
    EXPLAIN SELECT count() FROM t_join_lambda_left ANY LEFT JOIN t_join_lambda_right ON t_join_lambda_left.k = t_join_lambda_right.k
    WHERE t_join_lambda_right.v = 12345 OR arrayExists(x -> rand(x) % 2 = 0, materialize([1]))
) WHERE explain LIKE '%Strictness: semi%' OR explain LIKE '%Type: inner%';

-- A deterministic lambda still allows the conversion.
SELECT 'deterministic lambda';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM t_join_lambda_left LEFT JOIN t_join_lambda_right ON t_join_lambda_left.k = t_join_lambda_right.k
    WHERE t_join_lambda_right.v = 12345 AND arrayExists(x -> x % 2 = 0, materialize([1]))
) WHERE explain LIKE '%Type: inner%';

DROP TABLE t_join_lambda_left;
DROP TABLE t_join_lambda_right;

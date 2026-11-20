-- A lambda that captures no non-constant column is folded into a constant `ColumnFunction`, which makes the
-- higher-order function around it constant as well when its other arguments are constant. A constant is
-- evaluated once and its value is reused for the whole query, so a non-deterministic call in the lambda body
-- was drawn a single time and decided every row: `WHERE arrayExists(y -> rand(y) % 2 = 0, [1])` either kept
-- all the rows or dropped all of them. The lambda body has to be looked at, and the body of a lambda nested
-- in another lambda as well - it is such a folded carrier inside the outer lambda's `ActionsDAG`.

-- The plan is what is asserted here, and it is a plan of the analyzer; parallel replicas build another one.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

-- 100 blocks, so both values occur unless a single draw is frozen for the whole query (2^-99).
SELECT uniqExact(arrayExists(y -> rand(y) % 2 = 0, [1])) = 2 FROM numbers(100000) SETTINGS max_block_size = 1000;
SELECT uniqExact(arrayExists(x -> arrayExists(y -> rand(y) % 2 = 0, [1]), [1])) = 2 FROM numbers(100000) SETTINGS max_block_size = 1000;

-- The condition stays a function in the plan instead of becoming a folded constant.
SELECT count() FROM (
    EXPLAIN actions = 1 SELECT count() FROM numbers(10) WHERE arrayExists(y -> rand(y) % 2 = 0, [1])
) WHERE explain LIKE '%Filter column: arrayExists%';

SELECT count() FROM (
    EXPLAIN actions = 1 SELECT count() FROM numbers(10) WHERE arrayExists(x -> arrayExists(y -> rand(y) % 2 = 0, [1]), [1])
) WHERE explain LIKE '%Filter column: arrayExists%';

-- A deterministic lambda over a constant array is still folded, nested or not.
SELECT 'deterministic lambda';
SELECT count() FROM (
    EXPLAIN actions = 1 SELECT count() FROM numbers(10) WHERE arrayExists(x -> arrayExists(y -> y % 2 = 0, [1]), [1])
) WHERE explain LIKE '%Filter column: 0%';

SELECT arrayExists(y -> y % 2 = 0, [1, 2]);
SELECT arrayMap(x -> arrayExists(y -> y % 2 = 0, [1]), range(3));

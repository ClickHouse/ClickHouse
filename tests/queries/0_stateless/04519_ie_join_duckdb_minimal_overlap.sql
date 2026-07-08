-- Ported from DuckDB test/sql/join/iejoin/test_iejoin_sort_tasks.test_slow, scaled down
-- from 10M to 200k rows per side: two adjacent ranges of unit intervals with exactly one
-- matching pair at the boundary.

SET allow_experimental_ie_join = 1;

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT lhs.b, rhs.b
    FROM (SELECT toInt64(number + 1) AS b, toInt64(number + 2) AS e FROM numbers(200001)) lhs
    JOIN (SELECT toInt64(number + 200001) AS b, toInt64(number + 200002) AS e FROM numbers(200001)) rhs
    ON lhs.b < rhs.e AND rhs.b < lhs.e
) WHERE explain LIKE '%IEJoin%';

SELECT lhs.b, rhs.b
FROM (SELECT toInt64(number + 1) AS b, toInt64(number + 2) AS e FROM numbers(200001)) lhs
JOIN (SELECT toInt64(number + 200001) AS b, toInt64(number + 200002) AS e FROM numbers(200001)) rhs
ON lhs.b < rhs.e AND rhs.b < lhs.e;

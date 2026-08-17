-- Tags: no-old-analyzer

-- Two adjacent ranges of unit intervals (200k rows per side) with exactly one matching
-- pair at the boundary.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

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

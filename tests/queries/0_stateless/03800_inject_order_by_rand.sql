SET allow_experimental_analyzer = 0;
SET inject_random_order_for_select_without_order_by = 1;

-- single select: expect a Sorting step injected
SELECT count()
FROM (EXPLAIN PLAN SELECT number FROM numbers(5))
WHERE explain LIKE '%Sorting%';

-- single select with explicit ORDER BY: no injection
SELECT count()
FROM (EXPLAIN PLAN SELECT number FROM numbers(5) ORDER BY number)
WHERE explain LIKE '%Sorting%';

-- union: each child SELECT should get ORDER BY rand()
SELECT count()
FROM (
    EXPLAIN PLAN
    SELECT number FROM numbers(5)
    UNION ALL
    SELECT number FROM numbers(5)
)
WHERE explain LIKE '%Sorting%';

-- disable the feature flag: no injection
SET inject_random_order_for_select_without_order_by = 0;

SELECT count()
FROM (EXPLAIN PLAN SELECT number FROM numbers(5))
WHERE explain LIKE '%Sorting%';

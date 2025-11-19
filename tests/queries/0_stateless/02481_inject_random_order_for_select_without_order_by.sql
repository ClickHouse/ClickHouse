-- A test for setting `inject_random_order_for_select_without_order_by`

-- The setting is disabled by default, enable it for the test.
SET inject_random_order_for_select_without_order_by = 1;

-- Works only with enabled analyzer
SET enable_analyzer = 1;

-- If enabled, `ORDER BY rand()` is injected into the query plan.
-- We can not test the query result directly (because of randomization), we test the presence
-- or absence of sorting operators in the query plan.

SELECT 'Simple SELECT: expect a Sorting step injected';
SELECT count()
FROM (EXPLAIN PLAN SELECT number FROM numbers(5))
WHERE explain LIKE '%Sorting%';

SELECT 'Simple SELECT with ORDER BY: no 2nd ORDER BY injected';
SELECT count()
FROM (EXPLAIN PLAN SELECT number FROM numbers(5) ORDER BY number)
WHERE explain LIKE '%Sorting%';

SELECT 'UNION: expect ORDER BY injected into each child';
SELECT count()
FROM (
    EXPLAIN PLAN
    SELECT number FROM numbers(5)
    UNION ALL
    SELECT number FROM numbers(5)
)
WHERE explain LIKE '%Sorting%';

-- Now disable the setting
SET inject_random_order_for_select_without_order_by = 0;

SELECT 'Simple SELECT: no ORDER BY injected';
SELECT count()
FROM (EXPLAIN PLAN SELECT number FROM numbers(5))
WHERE explain LIKE '%Sorting%';

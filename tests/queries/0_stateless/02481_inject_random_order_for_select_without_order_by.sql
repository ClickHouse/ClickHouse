-- A test for setting `inject_random_order_for_select_without_order_by`

-- The setting is disabled by default, enable it for the test.
SET inject_random_order_for_select_without_order_by = 1;

-- Works only with enabled analyzer
SET explain_query_plan_default = 'legacy';

-- If enabled, `SHUFFLE` is injected into the query plan.
-- We can not test the query result directly (because of randomization), we test the presence
-- or absence of shuffle and sorting operators in the query plan.

SELECT 'Simple SELECT: expect a Shuffle step injected';
SELECT count()
FROM (EXPLAIN PLAN SELECT number FROM numbers(5))
WHERE trimLeft(explain) LIKE 'Shuffle%';

SELECT 'Simple SELECT with ORDER BY: no SHUFFLE injected';
SELECT countIf(trimLeft(explain) LIKE 'Shuffle%'), countIf(explain LIKE '%Sorting%')
FROM (EXPLAIN PLAN SELECT number FROM numbers(5) ORDER BY number);

SELECT 'UNION: expect SHUFFLE injected into each child';
SELECT count()
FROM (
    EXPLAIN PLAN
    SELECT number FROM numbers(5)
    UNION ALL
    SELECT number FROM numbers(5)
)
WHERE trimLeft(explain) LIKE 'Shuffle%';

-- Now disable the setting
SET inject_random_order_for_select_without_order_by = 0;

SELECT 'Simple SELECT: no SHUFFLE injected';
SELECT count()
FROM (EXPLAIN PLAN SELECT number FROM numbers(5))
WHERE explain LIKE '%Shuffle%';

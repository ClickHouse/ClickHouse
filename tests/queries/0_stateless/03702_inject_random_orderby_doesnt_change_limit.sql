-- A test for setting `inject_random_order_for_select_without_order_by`

-- The setting is disabled by default, enable it for the test.
SET inject_random_order_for_select_without_order_by = 1;

-- Works only with enabled analyzer
SET enable_analyzer = 1;

SELECT 'Simple SELECT with limit: expect a Sorting step injected above limit';
EXPLAIN PLAN
SELECT number
FROM system.numbers
LIMIT 1;

SELECT 'UNION: expect ORDER BY injected as wrapping select above each child with limit';
EXPLAIN PLAN
SELECT number FROM system.numbers LIMIT 1
UNION ALL
SELECT number FROM system.numbers LIMIT 1;
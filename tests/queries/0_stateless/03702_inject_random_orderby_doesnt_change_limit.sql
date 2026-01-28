-- A test for Bug 88496

-- The setting is disabled by default, enable it for the test.
SET inject_random_order_for_select_without_order_by = 1;

-- Works only with enabled analyzer
SET enable_analyzer = 1;

-- Expect that these queries don't time out

SELECT number
FROM system.numbers
LIMIT 1;

SELECT number FROM system.numbers LIMIT 1
UNION ALL
SELECT number FROM system.numbers LIMIT 1;

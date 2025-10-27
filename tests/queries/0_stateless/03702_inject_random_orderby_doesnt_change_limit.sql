-- A test for setting `inject_random_order_for_select_without_order_by` with limits on sources

-- The setting is disabled by default, enable it for the test.
SET inject_random_order_for_select_without_order_by = 1;

-- Works only with enabled analyzer
SET enable_analyzer = 1;

SELECT 'Simple SELECT with limit: limit on source is unaffected by random order by';
SELECT number
FROM system.numbers
LIMIT 1;

SELECT 'UNION: limit in both subqueries are unaffected by random order by';
SELECT number FROM system.numbers LIMIT 1
UNION ALL
SELECT number FROM system.numbers LIMIT 1;
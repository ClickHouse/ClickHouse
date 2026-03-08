-- Quoted identifier `all` should be treated as a column reference,
-- not as the special ALL keyword in ORDER BY, GROUP BY, and LIMIT BY.

-- ORDER BY `all` as a column reference (quoted)
SELECT -number AS `all` FROM numbers(5) ORDER BY `all`;

-- ORDER BY ALL expands to all SELECT columns (unquoted)
SELECT number FROM numbers(5) ORDER BY ALL;

-- ORDER BY all (lowercase, unquoted) also expands
SELECT number FROM numbers(5) ORDER BY all;

-- ORDER BY `all` DESC
SELECT -number AS `all` FROM numbers(5) ORDER BY `all` DESC;

-- ORDER BY "all" (double-quoted) is also a column reference
SELECT -number AS `all` FROM numbers(5) ORDER BY "all";

-- GROUP BY `all` groups only by the column named "all"
SELECT number % 2 AS `all`, number % 3 AS b, count() FROM numbers(6) GROUP BY `all` ORDER BY `all`, b; -- { serverError NOT_AN_AGGREGATE }

-- GROUP BY ALL expands to all non-aggregate columns (unquoted)
SELECT number % 2 AS `all`, number % 3 AS b, count() FROM numbers(6) GROUP BY ALL ORDER BY `all`, b;

-- GROUP BY all (lowercase, unquoted) also expands
SELECT number % 2 AS `all`, number % 3 AS b, count() FROM numbers(6) GROUP BY all ORDER BY `all`, b;

-- GROUP BY "all" (double-quoted) is a column reference
SELECT number % 2 AS `all`, number % 3 AS b, count() FROM numbers(6) GROUP BY "all" ORDER BY `all`, b; -- { serverError NOT_AN_AGGREGATE }

-- LIMIT BY `all` limits by the column named "all" only
SELECT number % 3 AS `all`, number FROM numbers(9) ORDER BY `all`, number LIMIT 1 BY `all`;

-- LIMIT BY ALL limits by all columns (unquoted)
SELECT number % 3 AS `all`, number FROM numbers(9) ORDER BY `all`, number LIMIT 1 BY ALL;

-- LIMIT BY all (lowercase, unquoted) also expands
SELECT number % 3 AS `all`, number FROM numbers(9) ORDER BY `all`, number LIMIT 1 BY all;

-- LIMIT BY "all" (double-quoted) is a column reference
SELECT number % 3 AS `all`, number FROM numbers(9) ORDER BY `all`, number LIMIT 1 BY "all";

-- ORDER BY `all` with enable_order_by_all = 0 should still work as a column reference
SELECT -number AS `all` FROM numbers(5) ORDER BY `all` SETTINGS enable_order_by_all = 0;

-- ORDER BY ALL with enable_order_by_all = 0 treats ALL as a column name, but no such column exists
SELECT -number AS `all` FROM numbers(5) ORDER BY ALL SETTINGS enable_order_by_all = 0; -- { serverError UNKNOWN_IDENTIFIER }

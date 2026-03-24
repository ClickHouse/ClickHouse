-- Tags: no-fasttest, no-ordinary
SET allow_experimental_unique_predicate = 1;

-- All distinct rows: returns 1
SELECT UNIQUE(SELECT number FROM numbers(5));

-- Duplicate rows: returns 0
SELECT UNIQUE(SELECT number % 3 FROM numbers(6));

-- Empty subquery: returns 1 (vacuously unique)
SELECT UNIQUE(SELECT number FROM numbers(0));

-- Single row: returns 1
SELECT UNIQUE(SELECT 1);

-- All same values: returns 0
SELECT UNIQUE(SELECT 1 FROM numbers(3));

-- Multiple columns, all distinct
SELECT UNIQUE(SELECT number, number * 10 FROM numbers(5));

-- Multiple columns with duplicates
SELECT UNIQUE(SELECT number % 2, number % 3 FROM numbers(12));

-- UNIQUE in WHERE clause
SELECT 'in_where_true' WHERE UNIQUE(SELECT number FROM numbers(3));
SELECT 'in_where_false' WHERE UNIQUE(SELECT 1 FROM numbers(3));

-- NULL handling: rows with NULL are never considered duplicates (SQL standard)
SELECT UNIQUE(SELECT NULL FROM numbers(3));
SELECT UNIQUE(SELECT if(number % 2 = 0, NULL, number) FROM numbers(5));

-- Multiple columns with NULLs: rows with any NULL column are skipped
SELECT UNIQUE(SELECT number, if(number = 2, NULL, number) FROM numbers(5));

-- Nullable column with actual duplicates among non-NULL rows
SELECT UNIQUE(SELECT if(number < 3, number % 2, NULL) FROM numbers(5));

-- Correlated subquery: not supported (fails during identifier resolution)
SELECT UNIQUE(SELECT number FROM numbers(n)) FROM (SELECT 1 AS n); -- { serverError UNKNOWN_IDENTIFIER }

-- Disabled by default
SET allow_experimental_unique_predicate = 0;
SELECT UNIQUE(SELECT 1); -- { serverError SUPPORT_IS_DISABLED }

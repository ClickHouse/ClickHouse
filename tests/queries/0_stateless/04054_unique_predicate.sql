-- Tags: no-fasttest
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
SELECT UNIQUE(SELECT number % 2, number % 3 FROM numbers(6));

-- UNIQUE in WHERE clause
SELECT 'in_where_true' WHERE UNIQUE(SELECT number FROM numbers(3));
SELECT 'in_where_false' WHERE UNIQUE(SELECT 1 FROM numbers(3));

-- Disabled by default
SET allow_experimental_unique_predicate = 0;
SELECT UNIQUE(SELECT 1); -- { serverError SUPPORT_IS_DISABLED }

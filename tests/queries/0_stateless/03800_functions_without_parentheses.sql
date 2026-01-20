-- Tags: no-parallel

-- Test SQL standard niladic functions without parentheses

-- NOW and CURRENT_TIMESTAMP
SELECT toTypeName(NOW);
SELECT toTypeName(CURRENT_TIMESTAMP);
SELECT NOW = now();
SELECT CURRENT_TIMESTAMP = now();

-- Case insensitivity
SELECT toTypeName(now);
SELECT toTypeName(Now);
SELECT toTypeName(current_timestamp);

-- TODAY and CURRENT_DATE
SELECT toTypeName(TODAY);
SELECT toTypeName(CURRENT_DATE);
SELECT TODAY = today();
SELECT CURRENT_DATE = today();

-- CURRENT_USER
SELECT toTypeName(CURRENT_USER);
SELECT CURRENT_USER = currentUser();

-- CURRENT_DATABASE
SELECT toTypeName(CURRENT_DATABASE);
SELECT CURRENT_DATABASE = currentDatabase();

-- Verify they work in expressions
SELECT NOW + INTERVAL 1 DAY > NOW;
SELECT TODAY + 1 > TODAY;

-- Verify they work in WHERE clause
SELECT 1 WHERE NOW > '2000-01-01';
SELECT 1 WHERE TODAY > '2000-01-01';

-- Verify error for functions that don't allow omitting parentheses
SELECT concat; -- { serverError UNKNOWN_IDENTIFIER }


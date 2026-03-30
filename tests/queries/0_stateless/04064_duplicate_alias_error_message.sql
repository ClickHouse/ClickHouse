-- https://github.com/ClickHouse/ClickHouse/issues/62914
-- Should report MULTIPLE_EXPRESSIONS_FOR_ALIAS, not UNKNOWN_IDENTIFIER
SELECT number AS num, num * 1 AS num FROM numbers(10); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

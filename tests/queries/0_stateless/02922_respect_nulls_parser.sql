SELECT first_value(number) RESPECT NULLS IGNORE NULLS from numbers(1); -- { clientError SYNTAX_ERROR }

SELECT formatQuery('SELECT first_value(number) RESPECT NULLS from numbers(1)');
SELECT formatQuery('SELECT first_value(number) IGNORE NULLS from numbers(1)');
SELECT formatQuery('SELECT any (number) RESPECT NULLS from numbers(1)');
SELECT formatQuery('SELECT LAST_VALUE(number) RESPECT NULLS from numbers(1)');

-- The parser doesn't know if this function supports "RESPECT/IGNORE" NULLS
SELECT formatQuery('SELECT sum(number) RESPECT NULLS from numbers(1)');

-- Normal functions should throw in the server
SELECT toDateTimeNonExistingFunction(now()) RESPECT NULLS b; -- { serverError UNKNOWN_FUNCTION }
SELECT toDateTime(now()) RESPECT NULLS b; -- { serverError SYNTAX_ERROR }
SELECT count() from numbers(10) where in(number, (0)) RESPECT NULLS; -- { serverError SYNTAX_ERROR }
SELECT if(number > 0, number, 0) respect nulls from numbers(0); -- { serverError SYNTAX_ERROR }
WITH (x -> x + 1) AS lambda SELECT lambda(number) RESPECT NULLS FROM numbers(10) SETTINGS enable_analyzer = 1; -- { serverError SYNTAX_ERROR }
SELECT * from system.one WHERE indexHint(dummy = 1) RESPECT NULLS; -- { serverError SYNTAX_ERROR }
SELECT arrayJoin([[3,4,5], [6,7], [2], [1,1]]) IGNORE NULLS; -- { serverError SYNTAX_ERROR }
SELECT number, grouping(number % 2, number) RESPECT NULLS AS gr FROM numbers(10) GROUP BY GROUPING SETS ((number), (number % 2)) SETTINGS force_grouping_standard_compatibility = 0; -- { serverError SYNTAX_ERROR }

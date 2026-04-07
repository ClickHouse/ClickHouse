-- { echoOn }

-- The function name is case insensitive, with or without respect nulls and using any of the aliases
Select number, first_value (number) RESPECT NULLS over (order by number) from numbers(1);
Select number, First_value (number) RESPECT NULLS over (order by number) from numbers(1);
Select number, FIRST_VALUE (number) RESPECT NULLS over (order by number) from numbers(1);
Select number, FIRST_VALUE (number) over (order by number) from numbers(1);
Select number, first_value_respect_nulls (number) over (order by number) from numbers(1);
Select number, any (number) RESPECT NULLS over (order by number) from numbers(1);
Select number, any_value (number) RESPECT NULLS over (order by number) from numbers(1);

Select number, last_value (number) RESPECT NULLS over (order by number) from numbers(1);
Select number, Last_value (number) RESPECT NULLS over (order by number) from numbers(1);
Select number, LAST_VALUE (number) RESPECT NULLS over (order by number) from numbers(1);
Select number, LAST_VALUE (number) over (order by number) from numbers(1);
Select number, last_value_respect_nulls (number) over (order by number) from numbers(1);
Select number, anyLast (number) RESPECT NULLS over (order by number) from numbers(1);

-- IGNORE NULLS should be accepted too
Select number, FIRST_VALUE (number) IGNORE NULLS over (order by number) from numbers(1);
Select number, LAST_VALUE (number) IGNORE NULLS over (order by number) from numbers(1);

-- When applying IGNORE NULLs to first_value_respect_nulls we go back to the original function (any)
Select first_value_respect_nulls (number) IGNORE NULLS from (SELECT if(number < 2, NULL, number) as number FROM numbers(10));
Select FIRST_VALUE_respect_nulls (number) IGNORE NULLS from (SELECT if(number < 2, NULL, number) as number FROM numbers(10));
Select last_value_respect_nulls (number) IGNORE NULLS from (SELECT if(number < 2, NULL, number) as number FROM numbers(10));
Select LAST_VALUE_respect_nulls (number) IGNORE NULLS from (SELECT if(number < 2, NULL, number) as number FROM numbers(10));

-- IGNORE/RESPECT NULLS should work with combinators because we can do it
SELECT first_valueIf (number, NOT isNull(number) AND (assumeNotNull(number) > 5)) RESPECT NULLS from (SELECT if(number < 2, NULL, number) as number FROM numbers(10));
SELECT last_valueIf (number, NOT isNull(number) AND (assumeNotNull(number) > 5)) RESPECT NULLS from (SELECT if(number < 2, NULL, number) as number FROM numbers(10));
SELECT first_valueIf (number, isNull(number)) RESPECT NULLS from (SELECT if(number > 8, NULL, number) as number FROM numbers(10));
SELECT last_valueIf (number, isNull(number)) RESPECT NULLS from (SELECT if(number > 8, NULL, number) as number FROM numbers(10));
SELECT toTypeName(first_valueIfState(number, isNull(number)) RESPECT NULLS) from (SELECT if(number > 8, NULL, number) as number FROM numbers(10));
SELECT toTypeName(last_valueIfState(number, isNull(number)) RESPECT NULLS) from (SELECT if(number > 8, NULL, number) as number FROM numbers(10));
SELECT FIRST_VALUEIf (number, NOT isNull(number) AND (assumeNotNull(number) > 5)) RESPECT NULLS from (SELECT if(number < 2, NULL, number) as number FROM numbers(10));
SELECT LAST_VALUEIf (number, NOT isNull(number) AND (assumeNotNull(number) > 5)) RESPECT NULLS from (SELECT if(number < 2, NULL, number) as number FROM numbers(10));
SELECT anyIf (number, isNull(number)) RESPECT NULLS from (SELECT if(number > 8, NULL, number) as number FROM numbers(10));
SELECT anyLastIf (number, isNull(number)) RESPECT NULLS from (SELECT if(number > 8, NULL, number) as number FROM numbers(10));
SELECT toTypeName(FIRST_VALUEIfState(number, isNull(number)) RESPECT NULLS) from (SELECT if(number > 8, NULL, number) as number FROM numbers(10));
SELECT toTypeName(LAST_VALUEIfState(number, isNull(number)) RESPECT NULLS) from (SELECT if(number > 8, NULL, number) as number FROM numbers(10));

-- Unsupported functions should throw in the server
SELECT number, sum (number) RESPECT NULLS over (order by number) from numbers(1); -- { serverError NOT_IMPLEMENTED }
SELECT number, avgIf (number) RESPECT NULLS over (order by number) from numbers(1); -- { serverError NOT_IMPLEMENTED }
-- Same for double RESPECT NULLS
SELECT number, first_value_respect_nulls (number) RESPECT NULLS over (order by number) from numbers(1); -- { serverError NOT_IMPLEMENTED }
SELECT number, last_value_respect_nulls (number) RESPECT NULLS over (order by number) from numbers(1); -- { serverError NOT_IMPLEMENTED }

-- Aggregate_functions_null_for_empty should work the same way
SELECT toTypeName(any(number) RESPECT NULLS) from numbers(1);
SELECT toTypeName(anyOrNull(number) RESPECT NULLS) from numbers(1);
SELECT any(number) RESPECT NULLS from numbers(0);
SELECT anyOrNull(number) RESPECT NULLS from numbers(0);
SELECT any(number) RESPECT NULLS from (Select NULL::Nullable(UInt8) as number FROM numbers(10));
SELECT anyOrNull(number) RESPECT NULLS from (Select NULL::Nullable(UInt8) as number FROM numbers(10));
SELECT any(number) RESPECT NULLS from (Select if(number > 8, NULL, number) as number FROM numbers(10));
SELECT anyOrNull(number) RESPECT NULLS from (Select if(number > 8, NULL, number) as number FROM numbers(10));
SELECT any(number) RESPECT NULLS from (Select if(number < 8, NULL, number) as number FROM numbers(10));
SELECT anyOrNull(number) RESPECT NULLS from (Select if(number < 8, NULL, number) as number FROM numbers(10));

SELECT toTypeName(any(number) RESPECT NULLS) from numbers(1) SETTINGS aggregate_functions_null_for_empty = 1;
SELECT any(number) RESPECT NULLS from numbers(0) SETTINGS aggregate_functions_null_for_empty = 1;

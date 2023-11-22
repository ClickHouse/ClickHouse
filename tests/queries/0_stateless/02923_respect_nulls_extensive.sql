-- The function name is case insensitive, with or without respect nulls and using any of the aliases
Select 'FIRST VALUE RESPECT NULLS CASE SENSITIVE';
Select number, first_value (number) RESPECT NULLS over (order by number) from numbers(1);
Select number, First_value (number) RESPECT NULLS over (order by number) from numbers(1);
Select number, FIRST_VALUE (number) RESPECT NULLS over (order by number) from numbers(1);
Select number, FIRST_VALUE (number) over (order by number) from numbers(1);
Select number, first_value_respect_nulls (number) over (order by number) from numbers(1);
Select number, any (number) RESPECT NULLS over (order by number) from numbers(1);
Select number, any_value (number) RESPECT NULLS over (order by number) from numbers(1);

-- IGNORE NULLS should work too
Select 'FIRST VALUE IGNORE NULLS';
Select number, FIRST_VALUE (number) IGNORE NULLS over (order by number) from numbers(1);
-- When applying IGNORE NULLs to first_value_respect_nulls we go back to the original function (any)
Select first_value_respect_nulls (number) IGNORE NULLS from (SELECT if(number < 2, NULL, number) as number FROM numbers(10));
Select FIRST_VALUE_respect_nulls (number) IGNORE NULLS from (SELECT if(number < 2, NULL, number) as number FROM numbers(10));

-- IGNORE/RESPECT NULLS should work with combinators because we can do it
SELECT first_valueIf (number, NOT isNull(number) AND (assumeNotNull(number) > 5)) RESPECT NULLS from (SELECT if(number < 2, NULL, number) as number FROM numbers(10));
SELECT first_valueIf (number, isNull(number)) RESPECT NULLS from (SELECT if(number > 8, NULL, number) as number FROM numbers(10));
SELECT toTypeName(first_valueIfState(number, isNull(number)) RESPECT NULLS) from (SELECT if(number > 8, NULL, number) as number FROM numbers(10));
SELECT FIRST_VALUEIf (number, NOT isNull(number) AND (assumeNotNull(number) > 5)) RESPECT NULLS from (SELECT if(number < 2, NULL, number) as number FROM numbers(10));
SELECT FIRST_VALUEIf (number, isNull(number)) RESPECT NULLS from (SELECT if(number > 8, NULL, number) as number FROM numbers(10));
SELECT toTypeName(FIRST_VALUEIfState(number, isNull(number)) RESPECT NULLS) from (SELECT if(number > 8, NULL, number) as number FROM numbers(10));


-- Respect nulls + Respect Nulls == ERROR (as if you do any other unsupported aggregation)
-- Select number, first_value_respect_nulls (number) RESPECT NULLS over (order by number) from (SELECT nullIf(number < 2) FROM numbers(1)); -- { clientError NOT_IMPLEMENTED }
-- Select number, sum (number) RESPECT NULLS over (order by number) from (SELECT nullIf(number < 2) FROM numbers(1)); -- { clientError NOT_IMPLEMENTED }

SELECT first_value(number) RESPECT NULLS IGNORE NULLS from numbers(1); -- { clientError SYNTAX_ERROR }

SELECT formatQuery('Select first_value(number) RESPECT NULLS from numbers(1)');
SELECT formatQuery('Select first_value(number) IGNORE NULLS from numbers(1)');
SELECT formatQuery('Select any (number) RESPECT NULLS from numbers(1)');
SELECT formatQuery('Select LAST_VALUE(number) RESPECT NULLS from numbers(1)');

-- The parser doesn't know if this function supports "RESPECT/IGNORE" NULLS
SELECT formatQuery('Select sum(number) RESPECT NULLS from numbers(1)');

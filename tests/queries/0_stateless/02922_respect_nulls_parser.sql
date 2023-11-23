SELECT first_value(n) RESPECT NULLS IGNORE NULLS from numbers(1); -- { clientError SYNTAX_ERROR }

SELECT formatQuery('Select first_value(n) RESPECT NULLS from numbers(1)');
SELECT formatQuery('Select first_value(n) IGNORE NULLS from numbers(1)');
SELECT formatQuery('Select any (n) RESPECT NULLS from numbers(1)');
-- The parser doesn't need to know if this function supports "RESPECT/IGNORE" NULLS
SELECT formatQuery('Select sum(n) RESPECT NULLS from numbers(1)');

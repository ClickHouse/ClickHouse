-- Tags: no-fasttest

SELECT generateULID(1) != generateULID(2), toTypeName(generateULID());

-- The optional argument exists only to suppress common subexpression elimination, so its type
-- does not affect the result type.
SELECT toTypeName(generateULID(assumeNotNull(materialize(NULL))));
SELECT toTypeName(generateULID(NULL)), length(toString(generateULID(NULL)));
-- A NULL in a non-constant tag must not null out that row's identifier, so the count is 4, not 2.
SELECT any(toTypeName(generateULID(tag))), count(generateULID(tag)) FROM (SELECT if(number % 2, NULL, number) AS tag FROM numbers(4));

-- More than one argument is rejected whatever the argument types are.
SELECT generateULID(NULL, NULL); -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }
SELECT generateULID(1, 2); -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }

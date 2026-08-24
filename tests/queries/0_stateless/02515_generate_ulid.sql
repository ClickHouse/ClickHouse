-- Tags: no-fasttest

SELECT generateULID(1) != generateULID(2), toTypeName(generateULID());

-- The optional argument exists only to suppress common subexpression elimination, so its type
-- does not affect the result type.
SELECT toTypeName(generateULID(assumeNotNull(materialize(NULL))));
SELECT toTypeName(generateULID(NULL)), length(toString(generateULID(NULL)));

-- More than one argument is rejected whatever the argument types are.
SELECT generateULID(NULL, NULL); -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }
SELECT generateULID(1, 2); -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }

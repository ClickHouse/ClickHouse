-- Tags: no-fasttest

SELECT generateULID(1) != generateULID(2), toTypeName(generateULID());

-- The optional argument exists only to suppress common subexpression elimination, so its type
-- does not affect the result type.
SELECT toTypeName(generateULID(assumeNotNull(materialize(NULL))));

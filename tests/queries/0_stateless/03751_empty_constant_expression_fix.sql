-- Test for fix of issue #74241: Empty result column after evaluation of constant expression
-- The issue was that expressions like materialize(1) AND 0 could result in an empty column
-- when evaluated as constant expressions (e.g., as table function arguments).

-- Test that these expressions evaluate correctly
SELECT materialize(1) AND 0;
SELECT materialize(1) AND 1;
SELECT 1 AND materialize(0);
SELECT materialize(1) OR 0;
SELECT materialize(0) OR 1;

-- Test with nullable types
SELECT materialize(toNullable(1)) AND 0;
SELECT materialize(1) AND toNullable(0);

-- Test with low cardinality types
SELECT materialize(toLowCardinality(1)) AND 0;
SELECT materialize(1) AND toLowCardinality(0);

-- Test nested expressions
SELECT (materialize(16) AND (toLowCardinality(-1) AND 16) AND 1 AND (-0. AND 255) AND 16) AND -1 AND toNullable(16);

-- Test that the file table function returns proper error (BAD_ARGUMENTS, not LOGICAL_ERROR)
-- when given invalid constant expression as filename
SELECT 'Expected BAD_ARGUMENTS error:';
DESCRIBE TABLE file(materialize(1) AND 0); -- { serverError BAD_ARGUMENTS }

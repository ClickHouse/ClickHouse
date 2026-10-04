-- The `obfuscate` table function holds a SELECT query as its argument, so it has no column name
-- and cannot be used as an expression. It used to throw a logical error when used as an argument
-- of another table function (found by AST fuzzer), now it is a plain exception.

SELECT * FROM numbers_mt(obfuscate(SELECT 1), 3); -- { serverError UNKNOWN_FUNCTION }

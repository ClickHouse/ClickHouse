SELECT * FROM generateRandom('i8', 1, 10, 10); -- { serverError SYNTAX_ERROR }
SELECT * FROM generateRandom; -- { serverError UNKNOWN_TABLE }
SELECT * FROM generateRandom('i8 UInt8', 1, 10, 10, 10, 10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM generateRandom('', 1, 10, 10); -- { serverError SYNTAX_ERROR }
-- `DESCRIBE TABLE` parses the arguments before the analyzer resolves them, so a hand-written `_CAST`
-- reaches the argument check with any number of arguments, including none.
DESCRIBE TABLE generateRandom('i8 UInt8', _CAST()); -- { serverError BAD_ARGUMENTS }

SELECT * FROM generateRandom('i8', 1, 10, 10); -- { serverError SYNTAX_ERROR }
SELECT * FROM generateRandom; -- { serverError UNKNOWN_TABLE }
SELECT * FROM generateRandom('i8 UInt8', 1, 10, 10, 10, 10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM generateRandom('', 1, 10, 10); -- { serverError SYNTAX_ERROR }

SELECT * FROM generateRandom('i8', 1, 10, 10); -- { serverError SYNTAX_ERROR }
SELECT * FROM generateRandom; -- { serverError UNKNOWN_TABLE }
SELECT * FROM generateRandom('i8 UInt8', 1, 10, 10, 10, 10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM generateRandom('', 1, 10, 10); -- { serverError SYNTAX_ERROR }
-- `_CAST` takes a value and a type name; any other argument list is not a cast wrapper. The
-- analyzer rejects such a `_CAST` while resolving it, so the code differs per analyzer.
-- `LIMIT 1` bounds the query: `generateRandom` is an infinite source, so without it an argument that
-- is wrongly accepted would be reported by a test timeout rather than by an unexpected success.
SELECT * FROM generateRandom('i8 UInt8', _CAST()) LIMIT 1; -- { serverError BAD_ARGUMENTS, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM generateRandom('i8 UInt8', _CAST(1)) LIMIT 1; -- { serverError BAD_ARGUMENTS, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM generateRandom('i8 UInt8', _CAST(1, 'UInt64', 'extra')) LIMIT 1; -- { serverError BAD_ARGUMENTS, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

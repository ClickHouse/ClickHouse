SELECT JSONHas(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT JSONExtract('{"a":1}'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT JSONHas('{"a":1}', CAST('a' AS Nullable(String))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

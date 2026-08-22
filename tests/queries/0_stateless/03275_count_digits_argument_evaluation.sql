-- No arguments passed
SELECT countDigits(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- Invalid 1st argument passed
SELECT countDigits(toFloat32(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- More arguments then expected
SELECT countDigits(1, 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

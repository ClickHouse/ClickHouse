-- { echo }
-- No arguments passed
SELECT bitSlice(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- Invalid 1st argument passed
SELECT bitSlice(1, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Valid 1st argument, invalid 2nd argument passed
SELECT bitSlice('Hello', 'World'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Valid 1st argument & 2nd argument, invalid 3rd argument passed
SELECT bitSlice('Hello', 1, 'World'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- More arguments then expected
SELECT bitSlice('Hello', 1, 1, 'World'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

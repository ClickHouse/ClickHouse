SELECT current_schemas(true) AS result;
SELECT current_schemas(false) AS result;
SELECT current_schemas(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT current_schemas(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
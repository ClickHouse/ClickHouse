SELECT current_schemas(true) AS result;
SELECT current_schemas(false) AS result;
SELECT current_schemas(1); -- { serverError 43 }
SELECT current_schemas(); -- { serverError 42 }
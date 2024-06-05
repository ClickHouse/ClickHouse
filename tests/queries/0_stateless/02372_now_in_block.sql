SELECT count() FROM (SELECT DISTINCT nowInBlock(), nowInBlock('Pacific/Pitcairn') FROM system.numbers LIMIT 2);
SELECT nowInBlock(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT nowInBlock(NULL) IS NULL;
SELECT nowInBlock('UTC', 'UTC'); -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }

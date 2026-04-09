SET max_rows_to_read = 0, max_bytes_to_read = 0;

SELECT count() FROM (SELECT DISTINCT nowInBlock(), nowInBlock('Pacific/Pitcairn') FROM system.numbers LIMIT 2);
SELECT nowInBlock(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT nowInBlock(NULL) IS NULL;
SELECT nowInBlock('UTC', 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

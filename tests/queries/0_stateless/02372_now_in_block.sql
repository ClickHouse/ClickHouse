SELECT count() FROM (SELECT DISTINCT nowInBlock(), nowInBlock('Pacific/Pitcairn') FROM system.numbers LIMIT 2);
SELECT nowInBlock(1); -- { serverError 43 }
SELECT nowInBlock(NULL) IS NULL;
SELECT nowInBlock('UTC', 'UTC'); -- { serverError 42 }

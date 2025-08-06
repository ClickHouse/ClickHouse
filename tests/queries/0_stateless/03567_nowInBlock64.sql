SET max_rows_to_read = 0, max_bytes_to_read = 0;

SELECT nowInBlock64(3, 'America/Sao_Paulo', 3); --{serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION}
SELECT nowInBlock64(10); --{serverError ARGUMENT_OUT_OF_BOUND}
SELECT nowInBlock64('string'); --{serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT nowInBlock64(3, true); --{serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT nowInBlock64(3, 3); --{serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT nowInBlock64(3, 'string'); --{serverError BAD_ARGUMENTS}

SELECT count() FROM (SELECT DISTINCT nowInBlock64(), nowInBlock64(3), nowInBlock64(3, 'Pacific/Pitcairn') FROM system.numbers LIMIT 3);
SELECT nowInBlock64(NULL) IS NULL;

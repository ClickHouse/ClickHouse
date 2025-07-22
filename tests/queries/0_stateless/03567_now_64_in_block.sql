SET max_rows_to_read = 0, max_bytes_to_read = 0;

select now64InBlock(); --{serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select now64InBlock(3, 'America/Sao_Paulo', 3); --{serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select now64InBlock(10); --{serverError ARGUMENT_OUT_OF_BOUND}
select now64InBlock('string'); --{serverError ILLEGAL_TYPE_OF_ARGUMENT}
select now64InBlock(3, true); --{serverError ILLEGAL_TYPE_OF_ARGUMENT}
select now64InBlock(3, 3); --{serverError ILLEGAL_TYPE_OF_ARGUMENT}
select now64InBlock(3, 'string'); --{serverError BAD_ARGUMENTS}

SELECT count() FROM (SELECT DISTINCT now64InBlock(3), now64InBlock(3, 'Pacific/Pitcairn') FROM system.numbers LIMIT 2);
SELECT nowInBlock(NULL) IS NULL;

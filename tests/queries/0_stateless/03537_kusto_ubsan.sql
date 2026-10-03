SET interval_output_format = 'kusto';
SELECT INTERVAL 3508 MONTH; -- { clientError BAD_ARGUMENTS }

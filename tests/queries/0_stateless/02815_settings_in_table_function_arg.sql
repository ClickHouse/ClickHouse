select * from numbers(SETTINGS max_threads = 1); -- { serverError BAD_ARGUMENTS }
select * from numbers(coalesce(coalesce(1024, coalesce(coalesce(executable('JSON', 'data String', SETTINGS max_command_execution_time = 100), 65535, coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(2147483647, '0.0000065537'), '65535')), NULL), 10, '1'), '0.0001048577')), NULL))), NULL)), 0) ; -- { serverError BAD_ARGUMENTS }



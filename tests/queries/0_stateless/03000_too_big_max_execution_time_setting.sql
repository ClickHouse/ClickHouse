select 1 settings max_execution_time = 9223372036854775808; -- {clientError BAD_ARGUMENTS}
select 1 settings max_execution_time = 9223372036854.775; -- {clientError BAD_ARGUMENTS}


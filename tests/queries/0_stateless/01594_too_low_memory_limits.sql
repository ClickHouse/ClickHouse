-- it is not mandatory to use existing table since it fails earlier, hence just a placeholder.

-- this format of INSERT SELECT pass settings attach settings to the SELECT query only
-- expecting UNKNOWN_TABLE, not the server unexpected termination
insert into placeholder_table_name select * from numbers_mt(65535) settings max_memory_usage=1, max_untracked_memory=1 ; -- { serverError 60 }

-- this is another format of INSERT SELECT, that pass these settings exactly for INSERT query not the SELECT
-- expecting SETTING_CONSTRAINT_VIOLATION, not the server unexpected termination
insert into placeholder_table_name select * from numbers_mt(65535) format Null settings max_memory_usage=1, max_untracked_memory=1 ; -- { serverError 452 }

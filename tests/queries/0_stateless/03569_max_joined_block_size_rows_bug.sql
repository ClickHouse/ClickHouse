set enable_analyzer=1;
select * from system.one, system.one settings max_joined_block_size_rows=0, joined_block_split_single_row = 0 format Null;
select * from system.one, system.one settings max_joined_block_size_rows=0, joined_block_split_single_row = 1 format Null; -- { serverError NOT_IMPLEMENTED }

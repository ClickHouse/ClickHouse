set enable_analyzer=1;
select * from system.one as t1 join system.one as t2 on t1.dummy = t2.dummy settings max_joined_block_size_rows=0, joined_block_split_single_row = 0 format Null;
select * from system.one as t1 join system.one as t2 on t1.dummy = t2.dummy settings max_joined_block_size_rows=0, joined_block_split_single_row = 1 format Null; -- { serverError NOT_IMPLEMENTED }

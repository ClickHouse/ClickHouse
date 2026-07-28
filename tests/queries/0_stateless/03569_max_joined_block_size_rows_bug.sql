set enable_analyzer=1;
select * from system.one, system.one settings max_joined_block_size_rows=0 format Null;

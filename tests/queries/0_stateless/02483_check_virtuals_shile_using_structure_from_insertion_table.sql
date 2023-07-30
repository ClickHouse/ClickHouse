-- Tags: no-parallel

drop table if exists test;
create table test (line String, _file String, _path String) engine=Memory;
insert into function file(02483_data.LineAsString) select 'Hello' settings engine_file_truncate_on_insert=1;
set use_structure_from_insertion_table_in_table_functions=2;
insert into test select *, _file, _path from file(02483_data.LineAsString);
select line, _file from test;
drop table test;

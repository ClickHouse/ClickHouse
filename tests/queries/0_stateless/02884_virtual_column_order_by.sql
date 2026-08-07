-- Tags: no-fasttest
insert into function file('02884_1.csv') select 1 as x settings engine_file_truncate_on_insert=1;
insert into function file('02884_2.csv') select 2 as x settings engine_file_truncate_on_insert=1;
select _file, * from file('02884_{1,2}.csv') order by _file settings max_threads=1;

-- Tags: no-fasttest, no-parallel
insert into table function file('data.jsonl', 'JSONEachRow', 'x UInt32') select * from numbers(10) SETTINGS engine_file_truncate_on_insert=1;
select * from file('data.jsonl') order by x;

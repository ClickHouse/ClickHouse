-- Tags: no-fasttest
insert into table function file('data.jsonl', 'JSONEachRow', 'x UInt32') select * from numbers(10);
select * from file('data.jsonl');

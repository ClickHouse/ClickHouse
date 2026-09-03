desc format(JSONEachRow, 
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
$$);

desc format(JSONEachRow, 'a String, b Int64',
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
$$);

select * from format(JSONEachRow, 'a String, b Int64',
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
$$);

desc format(CSV, '1,2,"[1,2,3]","[[\'abc\'], [], [\'d\', \'e\']]"');
desc format(CSV, 'a1 Int32, a2 UInt64, a3 Array(Int32), a4 Array(Array(String))', '1,2,"[1,2,3]","[[\'abc\'], [], [\'d\', \'e\']]"');
select * from format(CSV, '1,2,"[1,2,3]","[[\'abc\'], [], [\'d\', \'e\']]"');
select * from format(CSV, 'a1 Int32, a2 UInt64, a3 Array(Int32), a4 Array(Array(String))', '1,2,"[1,2,3]","[[\'abc\'], [], [\'d\', \'e\']]"');

drop table if exists test;

create table test as format(TSV, 'cust_id UInt128', '20210129005809043707\n123456789\n987654321');

select * from test;
desc table test;
drop table test;

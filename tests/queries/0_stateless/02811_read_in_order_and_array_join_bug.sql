drop table if exists test_array_joins;
drop table if exists v4test_array_joins;
create table test_array_joins
(
    id UInt64 default rowNumberInAllBlocks() + 1,
    arr_1 Array(String),
    arr_2 Array(String),
    arr_3 Array(String),
    arr_4 Array(String)
) engine = MergeTree order by id;

insert into test_array_joins (id,arr_1, arr_2, arr_3, arr_4)
SELECT number,array(randomPrintableASCII(3)),array(randomPrintableASCII(3)),array(randomPrintableASCII(3)),array(randomPrintableASCII(3))
from numbers(1000);
create view v4test_array_joins as SELECT * from test_array_joins where id != 10;
select * from v4test_array_joins array join arr_1, arr_2, arr_3, arr_4 where match(arr_4,'a') and id < 100 order by id format Null settings optimize_read_in_order = 1;


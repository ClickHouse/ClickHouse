drop table if exists test_table;

create table if not exists test_table
(
    `id` UInt64,
    `value` String
)
ORDER by id;

set async_insert = 1;
set wait_for_async_insert = 0;

insert into test_table values (1, 'a'), (2, 'b'), (3, 'c');
insert into test_table values (2, 'b'), (3, 'c'), (4, 'd');

system flush async insert queue test_table;

select count() from test_table;

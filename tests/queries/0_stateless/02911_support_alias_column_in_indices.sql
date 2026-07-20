-- Tags: no-parallel

drop database if exists 02911_support_alias_column_in_indices;
create database 02911_support_alias_column_in_indices;
use 02911_support_alias_column_in_indices;

create table test1
(
    c UInt32,
    a alias c + 1,
    index i (a) type minmax
)
engine = MergeTree
order by c
settings index_granularity = 8192, min_index_granularity_bytes = 1024, index_granularity_bytes = 10485760; -- default settings, prevent randomization in tests

insert into test1 select * from numbers(10);
insert into test1 select * from numbers(11, 20);

explain indexes = 1 select * from test1 where a > 10 settings enable_analyzer = 0;
explain indexes = 1 select * from test1 where a > 10 settings enable_analyzer = 1;

create table test2
(
    c UInt32,
    a1 alias c + 1,
    a2 alias a1 + 1,
    index i (a2) type minmax
)
engine = MergeTree
order by c
settings index_granularity = 8192, min_index_granularity_bytes = 1024, index_granularity_bytes = 10485760; -- default settings, prevent randomization in tests

insert into test2 select * from numbers(10);
insert into test2 select * from numbers(11, 20);

explain indexes = 1 select * from test2 where a2 > 15 settings enable_analyzer = 0;
explain indexes = 1 select * from test2 where a2 > 15 settings enable_analyzer = 1;

drop database 02911_support_alias_column_in_indices;

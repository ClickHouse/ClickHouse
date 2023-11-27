-- Tags: no-parallel

drop database if exists 02911_support_alias_column_in_indices;
create database 02911_support_alias_column_in_indices;
use 02911_support_alias_column_in_indices;

create table test
(
    x UInt32,
    y alias x + 1,
    index i_y (y) type minmax
) engine = MergeTree order by x;

insert into test select * from numbers(10);
insert into test select * from numbers(11, 20);

create table test1
(
    x UInt32,
    y1 alias x + 1,
    y2 alias y1 + 1,
    index i_y (y2) type minmax
) engine = MergeTree order by tuple();

insert into test1 select * from numbers(10);
insert into test1 select * from numbers(11, 20);

explain indexes = 1 select * from test where y > 10;
explain indexes = 1 select * from test1 where y2 > 15;

drop database 02911_support_alias_column_in_indices;

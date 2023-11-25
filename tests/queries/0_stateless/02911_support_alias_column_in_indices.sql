-- Tags: no-parallel

drop database if exists 02911_support_alias_column_in_indices;
create database 02911_support_alias_column_in_indices;
use 02911_support_alias_column_in_indices;

CREATE TABLE test
(
    x UInt32,
    y ALIAS x + 1,
    INDEX i_y (y) TYPE minmax
) ENGINE = MergeTree ORDER BY x;

insert into test select * from numbers(10);
insert into test select * from numbers(11, 20);

CREATE TABLE test1
(
    x UInt32,
    y1 ALIAS x + 1,
    y2 ALIAS y1 + 1,
    INDEX i_y (y2) TYPE minmax
) ENGINE = MergeTree ORDER BY tuple();

insert into test1 select * from numbers(10);
insert into test1 select * from numbers(11, 20);

explain indexes = 1 select * from test where y > 10;
explain indexes = 1 select * from test1 where y2 > 10;

drop database 02911_support_alias_column_in_indices;

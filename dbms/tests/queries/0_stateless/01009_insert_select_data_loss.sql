drop table if exists tab;
create table tab (x UInt64) engine = MergeTree order by tuple();

insert into tab select number as n from numbers(20) any inner join (select number * 10 as n from numbers(2)) using(n) settings any_join_distinct_right_table_keys = 1, max_block_size = 5;
select * from tab order by x;

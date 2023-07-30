drop table if exists tab;
create table tab (x UInt64) engine = MergeTree order by tuple();

insert into tab select n from (SELECT number AS n FROM numbers(20)) nums
semi left join (select number * 10 as n from numbers(2)) js2 using(n)
settings max_block_size = 5;
select * from tab order by x;

drop table tab;

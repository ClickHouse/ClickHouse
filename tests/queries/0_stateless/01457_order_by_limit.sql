drop table if exists order_by_another;

create table order_by_another (a Nullable(UInt64), b UInt64) Engine = MergeTree order by tuple();
insert into order_by_another values (1, 8), (1, 7), (1, 6), (1, 5), (1, 4), (1, 3), (1, 2), (1, 1);

select 'asc nulls last, asc';
select a, b from order_by_another order by a asc nulls last, b asc limit 4;

select 'asc nulls first, asc';
select a, b from order_by_another order by a asc nulls first, b asc limit 4;

select 'desc nulls last, asc';
select a, b from order_by_another order by a desc nulls last, b asc limit 4;

select 'desc nulls first, asc';
select a, b from order_by_another order by a desc nulls first, b asc limit 4;

select 'asc nulls last, desc';
select a, b from order_by_another order by a asc nulls last, b desc limit 4;

select 'asc nulls first, desc';
select a, b from order_by_another order by a asc nulls first, b desc limit 4;

select 'desc nulls last, desc';
select a, b from order_by_another order by a desc nulls last, b desc limit 4;

select 'desc nulls first, desc';
select a, b from order_by_another order by a desc nulls first, b desc limit 4;

drop table if exists order_by_another;
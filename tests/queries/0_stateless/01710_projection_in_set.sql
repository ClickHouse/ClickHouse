-- Tags: no-s3-storage
drop table if exists x;
create table x (i UInt64, j UInt64, k UInt64, projection agg (select sum(j), avg(k) group by i), projection norm (select j, k order by i)) engine MergeTree order by tuple();

insert into x values (1, 2, 3);

set allow_experimental_projection_optimization = 1, use_index_for_in_with_subqueries = 0;

select sum(j), avg(k) from x where i in (select number from numbers(4));

select j, k from x where i in (select number from numbers(4));

drop table x;

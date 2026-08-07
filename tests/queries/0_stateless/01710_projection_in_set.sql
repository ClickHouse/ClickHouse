drop table if exists x;
create table x (i UInt64, j UInt64, k UInt64, projection agg (select sum(j), avg(k) group by i), projection norm (select j, k order by i)) engine MergeTree order by tuple();

insert into x values (1, 2, 3);

set optimize_use_projections = 1, use_index_for_in_with_subqueries = 0;

select sum(j), avg(k) from x where i in (select number from numbers(4));

select j, k from x where i in (select number from numbers(4));

drop table x;

-- Projection analysis should not break other IN constructs. See https://github.com/ClickHouse/ClickHouse/issues/35336
create table if not exists flows (SrcAS UInt32, Bytes UInt64) engine MergeTree() order by tuple();

insert into table flows values (15169, 83948), (12322, 98989), (60068, 99990), (15169, 89898), (15169, 83948), (15169, 89898), (15169, 83948), (15169, 89898), (15169, 83948), (15169, 89898), (15169, 83948), (15169, 89898);

select if(SrcAS in (select SrcAS from flows group by SrcAS order by sum(Bytes) desc limit 10) , SrcAS, 33) as SrcAS from flows where 2 == 2 order by SrcAS;

drop table flows;

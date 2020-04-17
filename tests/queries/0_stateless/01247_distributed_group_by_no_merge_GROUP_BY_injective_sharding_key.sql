drop table if exists dist_01247;
drop table if exists data_01247;

create table data_01247 as system.numbers engine=Memory();
insert into data_01247 select * from system.numbers limit 2;
create table dist_01247 as data_01247 engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01247, number);
-- since data is not inserted via distributed it will have duplicates
-- (and this is how we ensure that this optimization will work)

set max_distributed_connections=1;

select '-';
select * from dist_01247;

select 'optimize_skip_unused_shards';
set optimize_skip_unused_shards=1;
select * from dist_01247;

select 'GROUP BY number';
select count(), * from dist_01247 group by number;

-- dumb, but should work, since "GROUP BY 1" optimized out
select 'GROUP BY number, 1';
select count(), * from dist_01247 group by number, 1;
select 'GROUP BY 1';
select count(), min(number) from dist_01247 group by 1;

select 'GROUP BY number ORDER BY number DESC';
select count(), * from dist_01247 group by number order by number desc;

select 'GROUP BY toString(number)';
select count(), * from dist_01247 group by toString(number);

select 'GROUP BY number%2';
select count(), any(number) from dist_01247 group by number%2;

select 'countDistinct';
select count(DISTINCT number) from dist_01247;

select 'countDistinct GROUP BY number';
select count(DISTINCT number) from dist_01247 group by number;

select 'DISTINCT';
select DISTINCT number from dist_01247;

select 'HAVING';
select count() cnt, * from dist_01247 group by number having cnt < 0;

select 'LIMIT';
select count(), * from dist_01247 group by number limit 1;
select count(), * from dist_01247 group by number limit 1 offset 1;

select 'LIMIT BY';
select count(), * from dist_01247 group by number limit 0 by number;
select count(), * from dist_01247 group by number limit 1 by number;

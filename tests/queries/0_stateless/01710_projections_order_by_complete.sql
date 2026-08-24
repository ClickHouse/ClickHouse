-- Test from https://github.com/ClickHouse/ClickHouse/issues/37673

drop table if exists  data_proj_order_by_comp;
create table data_proj_order_by_comp (t UInt64, projection tSort (select * order by t)) ENGINE MergeTree() order by tuple();

system stop merges data_proj_order_by_comp;

insert into data_proj_order_by_comp values (5);
insert into data_proj_order_by_comp values (5);
insert into data_proj_order_by_comp values (6);

-- { echoOn }
select t from data_proj_order_by_comp where t > 0 order by t settings optimize_read_in_order=1;
select t from data_proj_order_by_comp where t > 0 order by t settings optimize_read_in_order=0;
select t from data_proj_order_by_comp where t > 0 order by t settings max_threads=1;
-- { echoOff }

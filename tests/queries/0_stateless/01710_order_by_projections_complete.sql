drop table if exists  data_order_by_proj_comp;
create table data_order_by_proj_comp (t UInt64, projection tSort (select * order by t)) ENGINE MergeTree() order by t;

system stop merges data_order_by_proj_comp;

insert into data_order_by_proj_comp values (5);
insert into data_order_by_proj_comp values (5);
insert into data_order_by_proj_comp values (6);

-- { echoOn }
select t from data_order_by_proj_comp where t > 0 order by t settings optimize_read_in_order=1;
select t from data_order_by_proj_comp where t > 0 order by t settings optimize_read_in_order=0;
select t from data_order_by_proj_comp where t > 0 order by t settings max_threads=1;
-- { echoOff }

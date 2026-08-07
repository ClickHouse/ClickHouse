drop table if exists  data_proj_order_by_incomp;
create table data_proj_order_by_incomp (t UInt64) ENGINE MergeTree() order by tuple();

system stop merges data_proj_order_by_incomp;

insert into data_proj_order_by_incomp values (5);
insert into data_proj_order_by_incomp values (5);

alter table data_proj_order_by_incomp add projection tSort (select * order by t);
insert into data_proj_order_by_incomp values (6);

-- { echoOn }
select t from data_proj_order_by_incomp where t > 0 order by t settings optimize_read_in_order=1;
select t from data_proj_order_by_incomp where t > 0 order by t settings optimize_read_in_order=0;
select t from data_proj_order_by_incomp where t > 0 order by t settings max_threads=1;
-- { echoOff }

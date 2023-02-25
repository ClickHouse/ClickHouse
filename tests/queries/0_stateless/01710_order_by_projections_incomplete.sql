drop table if exists  data_order_by_proj_incomp;
create table data_order_by_proj_incomp (t UInt64) ENGINE MergeTree() order by t;

system stop merges data_order_by_proj_incomp;

insert into data_order_by_proj_incomp values (5);
insert into data_order_by_proj_incomp values (5);

alter table data_order_by_proj_incomp add projection tSort (select * order by t);
insert into data_order_by_proj_incomp values (6);

-- { echoOn }
select t from data_order_by_proj_incomp where t > 0 order by t settings optimize_read_in_order=1;
select t from data_order_by_proj_incomp where t > 0 order by t settings optimize_read_in_order=0;
select t from data_order_by_proj_incomp where t > 0 order by t settings max_threads=1;
-- { echoOff }

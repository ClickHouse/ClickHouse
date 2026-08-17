drop table if exists data;
create table data (key Int) engine=MergeTree() order by key;
select 1 from merge(currentDatabase(), '^data$') prewhere _table in (NULL); -- { serverError ILLEGAL_PREWHERE }
select 1 from merge(currentDatabase(), '^data$') where _table in (NULL);
drop table data;

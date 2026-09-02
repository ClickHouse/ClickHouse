drop table if exists data;
create table data (part Int) engine=MergeTree() order by tuple() partition by part;
insert into data values (1)(2);

-- { echoOn }
select * from data prewhere indexHint(_partition_id = '1');
-- TODO: optimize_use_implicit_projections ignores indexHint (with analyzer) because source columns might be aliased.
select count() from data prewhere indexHint(_partition_id = '1') settings optimize_use_implicit_projections = 0;
select * from data where indexHint(_partition_id = '1');
select count() from data where indexHint(_partition_id = '1') settings optimize_use_implicit_projections = 0;

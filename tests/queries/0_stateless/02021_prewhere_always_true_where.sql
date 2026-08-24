drop table if exists data_02021;
create table data_02021 (key Int) engine=MergeTree() order by key;
insert into data_02021 values (1);
select count() from data_02021 prewhere 1 or ignore(key) where ignore(key)=0;
drop table data_02021;

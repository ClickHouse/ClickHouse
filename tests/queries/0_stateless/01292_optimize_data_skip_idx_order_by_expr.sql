drop table if exists data_01292;

create table data_01292 (
    key Int,
    index key_idx (key) type minmax granularity 1
) Engine=MergeTree() ORDER BY (key+0);

insert into data_01292 values (1);

optimize table data_01292 final;

select * from data_01292 where key > 0;

drop table if exists data_01292;

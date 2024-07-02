SET allow_experimental_analyzer=0; -- FIXME: analyzer is not supported yet

drop table if exists data_r1;
drop table if exists data_r2;
drop table if exists data;

create table data (key Int, part Int, value Int) engine=MergeTree() order by key partition by (part, key % 50);

create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;
create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;

insert into data select number key, intDiv(number, 10) part, number value from numbers(10);
insert into data_r1 select number key, intDiv(number, 10) part, number value from numbers(10);

ALTER TABLE data_r1 MOVE PARTITION (0, 1) TO TABLE data; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE data MOVE PARTITION (0, 1) TO TABLE data_r1; -- { serverError NOT_IMPLEMENTED }

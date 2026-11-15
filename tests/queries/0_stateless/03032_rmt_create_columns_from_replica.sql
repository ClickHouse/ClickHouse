drop table if exists data_r1;
drop table if exists data_r2;
create table data_r1 (key Int) engine=ReplicatedMergeTree('/tables/{database}', 'r1') order by tuple();
create table data_r2 engine=ReplicatedMergeTree('/tables/{database}', 'r2') order by tuple();
show create data_r2 format LineAsString;

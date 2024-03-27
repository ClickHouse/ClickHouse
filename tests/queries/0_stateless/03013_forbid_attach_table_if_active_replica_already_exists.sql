drop database if exists at;
create database at engine = Atomic;

drop table if exists at.t1 sync;
drop table if exists at.t2 sync;

create table at.t1 (a Int)
    engine=ReplicatedMergeTree('/clickhouse/tables/{database}/test', 'r1')
    order by tuple() SETTINGS index_granularity = 8192;
attach table at.t2 UUID '6c32d92e-bebf-4730-ae73-c43e5748f829'
       (a Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/test', 'r1')
       order by tuple() SETTINGS index_granularity = 8192; -- { serverError REPLICA_ALREADY_EXISTS };

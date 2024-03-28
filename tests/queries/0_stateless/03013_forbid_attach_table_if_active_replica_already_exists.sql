drop database if exists atomic_db;
create database atomic_db engine = Atomic;

drop table if exists atomic_db.t1 sync;
drop table if exists atomic_db.t2 sync;

create table atomic_db.t1 (a Int)
    engine=ReplicatedMergeTree('/clickhouse/tables/{database}/test', 'r1')
    order by tuple() SETTINGS index_granularity = 8192;
attach table atomic_db.t2 UUID '6c32d92e-bebf-4730-ae73-c43e5748f829'
       (a Int) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/test', 'r1')
       order by tuple() SETTINGS index_granularity = 8192; -- { serverError REPLICA_ALREADY_EXISTS };

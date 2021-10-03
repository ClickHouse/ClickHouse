-- Tags: no-parallel
-- Tag no-parallel: creates database

drop database if exists db_01530_atomic sync;

create database db_01530_atomic Engine=Atomic;
create table db_01530_atomic.data (key Int) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/db_01530_atomic/data', 'test') order by key;
drop database db_01530_atomic sync;

create database db_01530_atomic Engine=Atomic;
create table db_01530_atomic.data (key Int) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/db_01530_atomic/data', 'test') order by key;
drop database db_01530_atomic sync;


set database_atomic_wait_for_drop_and_detach_synchronously=1;

create database db_01530_atomic Engine=Atomic;
create table db_01530_atomic.data (key Int) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/db_01530_atomic/data', 'test') order by key;
drop database db_01530_atomic;

create database db_01530_atomic Engine=Atomic;
create table db_01530_atomic.data (key Int) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/db_01530_atomic/data', 'test') order by key;
drop database db_01530_atomic;


set database_atomic_wait_for_drop_and_detach_synchronously=0;

create database db_01530_atomic Engine=Atomic;
create table db_01530_atomic.data (key Int) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/db_01530_atomic/data', 'test') order by key;
drop database db_01530_atomic;

create database db_01530_atomic Engine=Atomic;
create table db_01530_atomic.data (key Int) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/db_01530_atomic/data', 'test') order by key; -- { serverError 253; }

set database_atomic_wait_for_drop_and_detach_synchronously=1;

drop database db_01530_atomic sync;

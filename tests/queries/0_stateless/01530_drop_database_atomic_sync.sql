-- Tags: no-parallel
-- Tag no-parallel: creates database

drop database if exists {CLICKHOUSE_DATABASE_1:Identifier} sync;

create database {CLICKHOUSE_DATABASE_1:Identifier} Engine=Atomic;
USE {CLICKHOUSE_DATABASE_1:Identifier};
create table {CLICKHOUSE_DATABASE_1:Identifier}.data (key Int) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/db_01530_atomic/data', 'test') order by key;
drop database {CLICKHOUSE_DATABASE_1:Identifier} sync;

create database {CLICKHOUSE_DATABASE_1:Identifier} Engine=Atomic;
create table {CLICKHOUSE_DATABASE_1:Identifier}.data (key Int) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/db_01530_atomic/data', 'test') order by key;
drop database {CLICKHOUSE_DATABASE_1:Identifier} sync;


set database_atomic_wait_for_drop_and_detach_synchronously=1;

create database {CLICKHOUSE_DATABASE_1:Identifier} Engine=Atomic;
create table {CLICKHOUSE_DATABASE_1:Identifier}.data (key Int) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/db_01530_atomic/data', 'test') order by key;
drop database {CLICKHOUSE_DATABASE_1:Identifier};

create database {CLICKHOUSE_DATABASE_1:Identifier} Engine=Atomic;
create table {CLICKHOUSE_DATABASE_1:Identifier}.data (key Int) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/db_01530_atomic/data', 'test') order by key;
drop database {CLICKHOUSE_DATABASE_1:Identifier};


set database_atomic_wait_for_drop_and_detach_synchronously=0;

create database {CLICKHOUSE_DATABASE_1:Identifier} Engine=Atomic;
create table {CLICKHOUSE_DATABASE_1:Identifier}.data (key Int) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/db_01530_atomic/data', 'test') order by key;
drop database {CLICKHOUSE_DATABASE_1:Identifier};

create database {CLICKHOUSE_DATABASE_1:Identifier} Engine=Atomic;
create table {CLICKHOUSE_DATABASE_1:Identifier}.data (key Int) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/db_01530_atomic/data', 'test') order by key; -- { serverError REPLICA_ALREADY_EXISTS }

set database_atomic_wait_for_drop_and_detach_synchronously=1;

drop database {CLICKHOUSE_DATABASE_1:Identifier} sync;

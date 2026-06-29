-- Tags: no-fasttest
-- - no-fasttest -- no SQLite

set allow_deprecated_database_ordinary=1;
-- Suppress "Server has databases (for example `X`) with Ordinary engine, which was deprecated."
set send_logs_level='error';

create database {CLICKHOUSE_DATABASE_1:Identifier} engine=Ordinary;
select engine, is_external from system.databases where name = {CLICKHOUSE_DATABASE_1:String};
drop database {CLICKHOUSE_DATABASE_1:Identifier} sync;

create database {CLICKHOUSE_DATABASE_1:Identifier} engine=Atomic;
select engine, is_external from system.databases where name = {CLICKHOUSE_DATABASE_1:String};
drop database {CLICKHOUSE_DATABASE_1:Identifier} sync;

create database {CLICKHOUSE_DATABASE_1:Identifier} engine=Memory;
select engine, is_external from system.databases where name = {CLICKHOUSE_DATABASE_1:String};
drop database {CLICKHOUSE_DATABASE_1:Identifier} sync;

create database {CLICKHOUSE_DATABASE_1:Identifier} engine=Replicated('/test/{database}/rdb', 's1', 'r1');
select engine, is_external from system.databases where name = {CLICKHOUSE_DATABASE_1:String};
drop database {CLICKHOUSE_DATABASE_1:Identifier} sync;

create database {CLICKHOUSE_DATABASE_1:Identifier} engine=SQLite({CLICKHOUSE_DATABASE_1:String});
select engine, is_external from system.databases where name = {CLICKHOUSE_DATABASE_1:String};
drop database {CLICKHOUSE_DATABASE_1:Identifier} sync;

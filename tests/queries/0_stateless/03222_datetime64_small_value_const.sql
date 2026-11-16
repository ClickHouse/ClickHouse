-- Tags: shard, need-query-parameters
set session_timezone = 'UTC'; -- don't randomize the session timezone
SET enable_analyzer = 1;

select *, (select toDateTime64(0, 3)) from remote('127.0.0.1', system.one) settings prefer_localhost_replica=0;
select *, (select toDateTime64(5, 3)) from remote('127.0.0.1', system.one) settings prefer_localhost_replica=0;
select *, (select toDateTime64('1970-01-01 00:45:25.456789', 6)) from remote('127.0.0.1', system.one) settings prefer_localhost_replica=0;
select *, (select toDateTime64('1970-01-01 00:53:25.456789123', 9)) from remote('127.0.0.1', system.one) settings prefer_localhost_replica=0;
select *, (select toDateTime64(null,3)) from remote('127.0.0.1', system.one) settings prefer_localhost_replica=0;

create database if not exists {CLICKHOUSE_DATABASE_1:Identifier};

drop table if exists {CLICKHOUSE_DATABASE:Identifier}.dt64_03222;
drop table if exists {CLICKHOUSE_DATABASE_1:Identifier}.distr_03222_dt64;

create table {CLICKHOUSE_DATABASE:Identifier}.dt64_03222(id UInt64, dt DateTime64(3)) engine = MergeTree order by id;
create table {CLICKHOUSE_DATABASE_1:Identifier}.distr_03222_dt64 (id UInt64, dt DateTime64(3)) engine = Distributed(test_shard_localhost, {CLICKHOUSE_DATABASE:Identifier}, dt64_03222);

insert into {CLICKHOUSE_DATABASE:Identifier}.dt64_03222 values(1, toDateTime64('1970-01-01 00:00:00.000',3));
insert into {CLICKHOUSE_DATABASE:Identifier}.dt64_03222 values(2, toDateTime64('1970-01-01 00:00:02.456',3));
insert into {CLICKHOUSE_DATABASE:Identifier}.dt64_03222 values(3, toDateTime64('1970-01-01 00:00:04.811',3));
insert into {CLICKHOUSE_DATABASE:Identifier}.dt64_03222 values(4, toDateTime64('1970-01-01 00:10:05',3));
insert into {CLICKHOUSE_DATABASE:Identifier}.dt64_03222 values(5, toDateTime64(0,3));

--Output : 1,5 2,3,4 4 1,2,3,5 0 0 5
select id, dt from {CLICKHOUSE_DATABASE_1:Identifier}.distr_03222_dt64 where dt = (select toDateTime64(0,3)) order by id;
select id, dt from {CLICKHOUSE_DATABASE_1:Identifier}.distr_03222_dt64 where dt > (select toDateTime64(0,3)) order by id;
select id, dt from {CLICKHOUSE_DATABASE_1:Identifier}.distr_03222_dt64 where dt > (select toDateTime64('1970-01-01 00:10:00.000',3)) order by id;
select id, dt from {CLICKHOUSE_DATABASE_1:Identifier}.distr_03222_dt64 where dt < (select toDateTime64(5,3)) order by id;

select count(*) from {CLICKHOUSE_DATABASE_1:Identifier}.distr_03222_dt64 where dt > (select toDateTime64('2024-07-20 00:00:00',3));
select count(*) from {CLICKHOUSE_DATABASE_1:Identifier}.distr_03222_dt64 where dt > (select now());
select count(*) from {CLICKHOUSE_DATABASE_1:Identifier}.distr_03222_dt64 where dt < (select toDateTime64('2004-07-20 00:00:00',3));


drop table if exists {CLICKHOUSE_DATABASE_1:Identifier}.distr_03222_dt64;
drop table if exists {CLICKHOUSE_DATABASE:Identifier}.dt64_03222;
drop database if exists {CLICKHOUSE_DATABASE_1:Identifier};

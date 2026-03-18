-- Tags: shard
set session_timezone = 'UTC'; -- don't randomize the session timezone
SET allow_experimental_analyzer = 1;

select *, (select toDateTime64(0, 3)) from remote('127.0.0.1', system.one) settings prefer_localhost_replica=0;
select *, (select toDateTime64(5, 3)) from remote('127.0.0.1', system.one) settings prefer_localhost_replica=0;
select *, (select toDateTime64('1970-01-01 00:45:25.456789', 6)) from remote('127.0.0.1', system.one) settings prefer_localhost_replica=0;
select *, (select toDateTime64('1970-01-01 00:53:25.456789123', 9)) from remote('127.0.0.1', system.one) settings prefer_localhost_replica=0;
select *, (select toDateTime64(null,3)) from remote('127.0.0.1', system.one) settings prefer_localhost_replica=0;

create database if not exists shard_0;
create database if not exists shard_1;

drop table if exists shard_0.dt64_03222;
drop table if exists shard_1.dt64_03222;
drop table if exists distr_03222_dt64;

create table shard_0.dt64_03222(id UInt64, dt DateTime64(3)) engine = MergeTree order by id;
create table shard_1.dt64_03222(id UInt64, dt DateTime64(3)) engine = MergeTree order by id;
create table distr_03222_dt64 (id UInt64, dt DateTime64(3)) engine = Distributed(test_cluster_two_shards_different_databases, '', dt64_03222);

insert into shard_0.dt64_03222 values(1, toDateTime64('1970-01-01 00:00:00.000',3));
insert into shard_0.dt64_03222 values(2, toDateTime64('1970-01-01 00:00:02.456',3));
insert into shard_1.dt64_03222 values(3, toDateTime64('1970-01-01 00:00:04.811',3));
insert into shard_1.dt64_03222 values(4, toDateTime64('1970-01-01 00:10:05',3));
insert into shard_1.dt64_03222 values(5, toDateTime64(0,3));

--Output : 1,5 2,3,4 4 1,2,3,5 0 0 5
select id, dt from distr_03222_dt64 where dt = (select toDateTime64(0,3)) order by id;
select id, dt from distr_03222_dt64 where dt > (select toDateTime64(0,3)) order by id;
select id, dt from distr_03222_dt64 where dt > (select toDateTime64('1970-01-01 00:10:00.000',3)) order by id;
select id, dt from distr_03222_dt64 where dt < (select toDateTime64(5,3)) order by id;

select count(*) from distr_03222_dt64 where dt > (select toDateTime64('2024-07-20 00:00:00',3));
select count(*) from distr_03222_dt64 where dt > (select now());
select count(*) from distr_03222_dt64 where dt < (select toDateTime64('2004-07-20 00:00:00',3));


drop table if exists shard_0.dt64_03222;
drop table if exists shard_1.dt64_03222;
drop table if exists distr_03222_dt64;

drop database shard_0;
drop database shard_1;

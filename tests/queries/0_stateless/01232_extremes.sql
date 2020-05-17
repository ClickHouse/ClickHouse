set send_logs_level = 'error';
set extremes = 1;

select * from remote('127.0.0.1', numbers(2));
select '-';
select * from remote('127.0.0.{1,1}', numbers(2));
select '-';
select * from remote('127.0.0.{1,2}', numbers(2));
select '-';
select * from remote('127.0.0.{2,2}', numbers(2));
select '-';
select * from remote('127.0.0.2', numbers(2));
select '------';

select * from (select * from numbers(2) union all select * from numbers(3) union all select * from numbers(1)) order by number;
select '-';
select * from (select * from numbers(1) union all select * from numbers(2) union all select * from numbers(3)) order by number;
select '-';
select * from (select * from numbers(3) union all select * from numbers(1) union all select * from numbers(2)) order by number;
select '------';

create database if not exists shard_0;
create database if not exists shard_1;

drop table if exists shard_0.num_01232;
drop table if exists shard_0.num2_01232;
drop table if exists shard_1.num_01232;
drop table if exists shard_1.num2_01232;
drop table if exists distr;
drop table if exists distr2;

create table shard_0.num_01232 (number UInt64) engine = MergeTree order by number;
create table shard_1.num_01232 (number UInt64) engine = MergeTree order by number;
insert into shard_0.num_01232 select number from numbers(2);
insert into shard_1.num_01232 select number from numbers(3);
create table distr (number UInt64) engine = Distributed(test_cluster_two_shards_different_databases, '', num_01232);

create table shard_0.num2_01232 (number UInt64) engine = MergeTree order by number;
create table shard_1.num2_01232 (number UInt64) engine = MergeTree order by number;
insert into shard_0.num2_01232 select number from numbers(3);
insert into shard_1.num2_01232 select number from numbers(2);
create table distr2 (number UInt64) engine = Distributed(test_cluster_two_shards_different_databases, '', num2_01232);

select * from distr order by number;
select '-';
select * from distr2 order by number;

drop table if exists shard_0.num_01232;
drop table if exists shard_0.num2_01232;
drop table if exists shard_1.num_01232;
drop table if exists shard_1.num2_01232;
drop table if exists distr;
drop table if exists distr2;


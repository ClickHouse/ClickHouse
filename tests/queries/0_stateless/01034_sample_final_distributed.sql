-- Tags: distributed

set enable_parallel_replicas = 1;
set parallel_replicas_mode = 'sampling_key';
set max_parallel_replicas = 3;
set parallel_replicas_for_non_replicated_merge_tree = 1;

drop table if exists sample_final;
create table sample_final (CounterID UInt32, EventDate Date, EventTime DateTime, UserID UInt64, Sign Int8) engine = CollapsingMergeTree(Sign) order by (CounterID, EventDate, intHash32(UserID), EventTime) sample by intHash32(UserID) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into sample_final select number / (8192 * 4), toDate('2019-01-01'), toDateTime('2019-01-01 00:00:01') + number, number / (8192 * 2), number % 3 = 1 ? -1 : 1 from numbers(1000000);

select 'count';
select count() from sample_final;
select 'count final';
select count() from sample_final final;
select 'count sample';
select count() from sample_final sample 1/2;
select 'count sample final';
select count() from sample_final final sample 1/2;
select 'count final max_parallel_replicas';
set max_parallel_replicas=2;
select count() from remote('127.0.0.{2|3}', currentDatabase(), sample_final) final;

drop table if exists sample_final;

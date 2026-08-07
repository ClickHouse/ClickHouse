-- Tags: no-asan
-- no-asan: the flaky check complains that the test sometimes runs > 60 sec on asan builds

set allow_suspicious_codecs=1;

select 'Original bug: the same query executed multiple times yielded different results.';
select 'For unclear reasons this happened only in Release builds, not in Debug builds.';

drop table if exists bug_delta_gorilla;

create table bug_delta_gorilla
(value_bug UInt64 codec (Delta, Gorilla))
engine = MergeTree
order by tuple() SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'
as (select 0 from numbers(20000000));

select count(*)
from bug_delta_gorilla
where 0 <> value_bug;

select count(*)
from bug_delta_gorilla
where 0 <> value_bug;

select count(*)
from bug_delta_gorilla
where 0 <> value_bug;

drop table if exists bug_delta_gorilla;

select 'The same issue in a much smaller repro happens also in Debug builds';

create table bug_delta_gorilla (val UInt64 codec (Delta, Gorilla))
engine = MergeTree
order by val SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into bug_delta_gorilla values (0)(1)(3);
select * from bug_delta_gorilla;

drop table if exists bug_delta_gorilla;

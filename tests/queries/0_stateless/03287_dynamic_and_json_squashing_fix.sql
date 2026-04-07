set enable_json_type=1;
set enable_dynamic_type=1;

drop table if exists src;
drop table if exists dst;

create table src (d Dynamic) engine=Memory;
create table dst (d Dynamic) engine=MergeTree order by tuple();
insert into src select materialize(42)::Int64;
insert into src select 'Hello';
insert into dst select * from remote('127.0.0.2', currentDatabase(), src);
select isDynamicElementInSharedData(d) from dst;
drop table src;
drop table dst;

create table src (json JSON) engine=Memory;
create table dst (json JSON) engine=MergeTree order by tuple();
insert into src select '{"a" : 42}';
insert into src select '{"b" : 42}';
insert into dst select * from remote('127.0.0.2', currentDatabase(), src);
select JSONSharedDataPaths(json) from dst;
drop table src;
drop table dst;


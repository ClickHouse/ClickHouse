-- Tags: global

drop table if exists xp;
drop table if exists xp_d;

create table xp(A Date, B Int64, S String) Engine=MergeTree partition by toYYYYMM(A) order by B;
insert into xp select '2020-01-01', number , '' from numbers(100000);

create table xp_d as xp Engine=Distributed(test_shard_localhost, currentDatabase(), xp);

select count() from xp_d prewhere toYYYYMM(A) global in (select toYYYYMM(min(A)) from xp_d) where B > -1;

drop table if exists xp;
drop table if exists xp_d;

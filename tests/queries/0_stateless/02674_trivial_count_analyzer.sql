drop table if exists m3;
drop table if exists replacing_m3;

-- { echoOn }
set enable_analyzer=1;
set optimize_trivial_count_query=1;

create table m3(a Int64, b UInt64) Engine=MergeTree order by tuple();

select count() from m3;

insert into m3 values (0,0);
insert into m3 values (-1,1);

select trimBoth(explain) from (explain select count() from m3) where explain like '%ReadFromPreparedSource (Optimized trivial count)%';
select count() from m3;
select count(*) from m3;
select count(a) from m3;
select count(b) from m3;
select count() + 1 from m3;

drop table m3;

-- checking queries with FINAL
create table replacing_m3(a Int64, b UInt64) Engine=ReplacingMergeTree() order by (a, b);
SYSTEM STOP MERGES replacing_m3;

select count() from replacing_m3;

insert into replacing_m3 values (0,0);
insert into replacing_m3 values (0,0);
insert into replacing_m3 values (-1,1);
insert into replacing_m3 values (-2,2);

select trimBoth(explain) from (explain select count() from replacing_m3) where explain like '%ReadFromPreparedSource (Optimized trivial count)%';
select count() from replacing_m3;
select count(*) from replacing_m3;
select count(a) from replacing_m3;
select count(b) from replacing_m3;

select count() from replacing_m3 FINAL;
select count(a) from replacing_m3 FINAL;
select count(b) from replacing_m3 FINAL;

drop table replacing_m3;

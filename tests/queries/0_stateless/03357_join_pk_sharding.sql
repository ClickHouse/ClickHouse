-- Tags: long

SET allow_statistics_optimize = 0;
drop table if exists tab_l;
drop table if exists tab_m;
drop table if exists tab_r;

create table tab_l (a UInt32, b UInt32, c UInt32, d UInt32) engine = MergeTree order by (a * 2, b + c);
create table tab_m (a UInt32, b UInt32, c UInt32, d UInt32) engine = MergeTree order by (c + d, b * 2);
create table tab_r (a UInt32, b UInt32, c UInt32, d UInt32) engine = MergeTree order by (a * 2, c * 2);

insert into tab_l select number, number, number, number from numbers(1e6);
insert into tab_m select number, number, number, number from numbers(1e6);
insert into tab_r select number, number, number, number from numbers(1e6);

--select explain e from (explain actions = 1 )
--where e like '%ReadFromMergeTree%' or e like '%Expression%' or e like '%Join%' or e like '%Clauses%' or e like '%Sharding%';

set enable_analyzer=1;
set query_plan_join_swap_table=0;
set query_plan_join_shard_by_pk_ranges=1;
set allow_experimental_parallel_reading_from_replicas=0;

-- { echo On }

-- two tables
select * from tab_l l inner join tab_m m on l.a * 2 = m.c + m.d and l.d = m.a and l.b + l.c = m.b * 2 order by l.a limit 10 offset 999990;

select explain e from (explain actions = 1 select * from tab_l l inner join tab_m m on l.a * 2 = m.c + m.d and l.b + l.c = m.b * 2)
where e like '%ReadFromMergeTree%' or e like '%Expression%' or e like '%Join%' or e like '%Clauses%' or e like '%Sharding%';

-- three tables
select * from tab_l l inner join tab_m m on l.a * 2 = m.c + m.d and l.d = m.a and l.b + l.c = m.b * 2 inner join tab_r r on l.a * 2 = r.a * 2 and l.b + l.c = r.c * 2 and l.d = r.d order by l.a limit 10 offset 999990;

select explain e from (explain actions = 1 select * from tab_l l inner join tab_m m on l.a * 2 = m.c + m.d and l.d = m.a and l.b + l.c = m.b * 2 inner join tab_r r on l.a * 2 = r.a * 2 and l.b + l.c = r.c * 2 and l.d = r.d)
where e like '%ReadFromMergeTree%' or e like '%Expression%' or e like '%Join%' or e like '%Clauses%' or e like '%Sharding%';

--- three tables, where m table matches one key, so that r table can match only one key as well
select * from tab_l l inner join tab_m m on l.a * 2 = m.c + m.d and l.d = m.a inner join tab_r r on l.a * 2 = r.a * 2 and l.b + l.c = r.c * 2 and l.d = r.d order by l.a limit 10 offset 999990;

select explain e from (explain actions = 1 select * from tab_l l inner join tab_m m on l.a * 2 = m.c + m.d and l.d = m.a inner join tab_r r on l.a * 2 = r.a * 2 and l.b + l.c = r.c * 2 and l.d = r.d)
where e like '%ReadFromMergeTree%' or e like '%Expression%' or e like '%Join%' or e like '%Clauses%' or e like '%Sharding%';

--- three tables, right table matches one key
select * from tab_l l inner join tab_m m on l.a * 2 = m.c + m.d and l.d = m.a and l.b + l.c = m.b * 2 inner join tab_r r on l.a * 2 = r.a * 2 and l.d = r.d order by l.a limit 10 offset 999990;

select explain e from (explain actions = 1 select * from tab_l l inner join tab_m m on l.a * 2 = m.c + m.d and l.d = m.a and l.b + l.c = m.b * 2 inner join tab_r r on l.a * 2 = r.a * 2 and l.d = r.d)
where e like '%ReadFromMergeTree%' or e like '%Expression%' or e like '%Join%' or e like '%Clauses%' or e like '%Sharding%';

--- three tables, tab_m table matches noting, so right table can match both keys
select * from tab_l l inner join tab_m m on l.d = m.a inner join tab_r r on l.a * 2 = r.a * 2 and l.b + l.c = r.c * 2 and l.d = r.d order by l.a limit 10 offset 999990;

select explain e from (explain actions = 1 select * from tab_l l inner join tab_m m on l.d = m.a inner join tab_r r on l.a * 2 = r.a * 2 and l.b + l.c = r.c * 2 and l.d = r.d)
where e like '%ReadFromMergeTree%' or e like '%Expression%' or e like '%Join%' or e like '%Clauses%' or e like '%Sharding%';

set join_use_nulls=1;

-- two tables
select * from tab_l l right join tab_m m on l.a * 2 = m.c + m.d and l.d = m.a and l.b + l.c = m.b * 2 order by l.a limit 10 offset 999990;

select explain e from (explain actions = 1 select * from tab_l l right join tab_m m on l.a * 2 = m.c + m.d and l.b + l.c = m.b * 2)
where e like '%ReadFromMergeTree%' or e like '%Expression%' or e like '%Join%' or e like '%Clauses%' or e like '%Sharding%';

-- three tables
select * from tab_l l left join tab_m m on l.a * 2 = m.c + m.d and l.d = m.a and l.b + l.c = m.b * 2 left join tab_r r on l.a * 2 = r.a * 2 and l.b + l.c = r.c * 2 and l.d = r.d order by l.a limit 10 offset 999990;

select explain e from (explain actions = 1 select * from tab_l l left join tab_m m on l.a * 2 = m.c + m.d and l.d = m.a and l.b + l.c = m.b * 2 left join tab_r r on l.a * 2 = r.a * 2 and l.b + l.c = r.c * 2 and l.d = r.d)
where e like '%ReadFromMergeTree%' or e like '%Expression%' or e like '%Join%' or e like '%Clauses%' or e like '%Sharding%';

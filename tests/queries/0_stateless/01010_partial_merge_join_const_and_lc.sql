set partial_merge_join = 1;

select s1.x, s2.x from (select 1 as x) s1 left join (select 1 as x) s2 using x;
select * from (select materialize(2) as x) s1 left join (select 2 as x) s2 using x;
select * from (select 3 as x) s1 left join (select materialize(3) as x) s2 using x;
select * from (select toLowCardinality(4) as x) s1 left join (select 4 as x) s2 using x;
select * from (select 5 as x) s1 left join (select toLowCardinality(5) as x) s2 using x;

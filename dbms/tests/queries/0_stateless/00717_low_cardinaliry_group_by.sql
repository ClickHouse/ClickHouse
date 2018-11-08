set allow_experimental_low_cardinality_type = 1;

drop table if exists test.tab;
create table test.tab (a String, b StringWithDictionary) engine = MergeTree order by a;
insert into test.tab values ('a_1', 'b_1'), ('a_2', 'b_2');
select count() from test.tab;
select a from test.tab group by a order by a;
select b from test.tab group by b order by b;
select length(b) as l from test.tab group by l;
select sum(length(a)), b from test.tab group by b order by b;
select sum(length(b)), a from test.tab group by a order by a;
select a, b from test.tab group by a, b order by a, b;
select sum(length(a)) from test.tab group by b, b || '_';
select length(b) as l from test.tab group by l;
select length(b) as l from test.tab group by l, l + 1;
select length(b) as l from test.tab group by l, l + 1, l + 2;
select length(b) as l from test.tab group by l, l + 1, l + 2, l + 3;
select length(b) as l from test.tab group by l, l + 1, l + 2, l + 3, l + 4;
select length(b) as l from test.tab group by l, l + 1, l + 2, l + 3, l + 4, l + 5;
select a, length(b) as l from test.tab group by a, l, l + 1 order by a;
select b, length(b) as l from test.tab group by b, l, l + 1 order by b;
select a, b, length(b) as l from test.tab group by a, b, l, l + 1 order by a, b;
drop table if exists test.tab;


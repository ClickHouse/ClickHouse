select s.a as a, s.a + 1 as b from (select 10 as a) s;
select s.a + 1 as a, s.a as b from (select 10 as a) s;
select s.a + 1 as a, s.a + 1 as b from (select 10 as a) s;
select s.a + 1 as b, s.a + 2 as a from (select 10 as a) s;
select s.a + 2 as b, s.a + 1 as a from (select 10 as a) s;

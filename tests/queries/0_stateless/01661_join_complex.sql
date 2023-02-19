select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on t1.b = t2.b and t1.c = t2.b and t1.d = t2.b or t1.e = t2.e;
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on t1.d = t2.b or t1.c = t2.b or t1.d = t2.b and t1.d = t2.b or t1.e = t2.e and t1.a=t2.a and t2.f=t1.f;

select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 left join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on t1.d = t2.b or t1.c = t2.b or t1.d = t2.b and t1.d = t2.b or (t1.e = t2.e and t1.a=t2.a and t2.f=t1.f);
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 right join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on t1.d = t2.b or t1.c = t2.b or t1.e = t2.e;
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on t1.d = t2.b or t1.c = t2.b or t1.e = t2.e and t1.a=t2.a and t2.f=t1.f;
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on t1.d = t2.b AND t1.e = t2.e OR t1.c = t2.b AND t1.e = t2.e OR t1.d = t2.b AND t1.f=t2.f OR t1.c = t2.b AND t1.f=t2.f;
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on (or(t1.d = t2.b and t1.e = t2.e, t1.d = t2.b and t1.f=t2.f, t1.c = t2.b and t1.e = t2.e, t1.c = t2.b and t1.f=t2.f));
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on (t1.d = t2.b and t1.e = t2.e or t1.d = t2.b and t1.f=t2.f or t1.c = t2.b and t1.e = t2.e or t1.c = t2.b and t1.f=t2.f);

select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 left join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on (t1.d = t2.b or t1.c = t2.b or t1.d = t2.b and t1.d = t2.b) or (t1.e = t2.e and t1.a=t2.a and t2.f=t1.f); -- { serverError INVALID_JOIN_ON_EXPRESSION }
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 right join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on (t1.d = t2.b or t1.c = t2.b) or t1.e = t2.e; -- { serverError INVALID_JOIN_ON_EXPRESSION }
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on (t1.d = t2.b or t1.c = t2.b) or (t1.e = t2.e and t1.a=t2.a and t2.f=t1.f); -- { serverError INVALID_JOIN_ON_EXPRESSION }
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on (t1.d = t2.b or t1.c = t2.b) and (t1.e = t2.e or t1.f=t2.f); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SET joined_subquery_requires_alias = 0;
SET max_threads = 1;

drop table if exists tab2;
drop table if exists tab3;

create table tab2 (a2 Int32, b2 Int32) engine = MergeTree order by a2;
create table tab3 (a3 Int32, b3 Int32) engine = MergeTree order by a3;

insert into tab2 values (2, 3);
insert into tab2 values (6, 4);
insert into tab3 values (2, 3);
insert into tab3 values (5, 4);
insert into tab3 values (100, 4);

select 'join on OR/AND chain';
select a2, b2, a3, b3 from tab2 any left join tab3 on a2=a3 and a2 +1 = b3 + 0 or b2=b3 and a2 +1 = b3 + 0 ;

drop table tab2;
drop table tab3;

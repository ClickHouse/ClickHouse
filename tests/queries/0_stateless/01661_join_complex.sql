select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on t1.b = t2.b and t1.c = t2.b and t1.d = t2.b or t1.e = t2.e;
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on t1.d = t2.b or t1.c = t2.b or t1.d = t2.b and t1.d = t2.b or t1.e = t2.e and t1.a=t2.a and t2.f=t1.f;

select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 left join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on t1.d = t2.b or t1.c = t2.b or t1.d = t2.b and t1.d = t2.b or (t1.e = t2.e and t1.a=t2.a and t2.f=t1.f);
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 right join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on t1.d = t2.b or t1.c = t2.b or t1.e = t2.e;
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on t1.d = t2.b or t1.c = t2.b or t1.e = t2.e and t1.a=t2.a and t2.f=t1.f;
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on t1.d = t2.b AND t1.e = t2.e OR t1.c = t2.b AND t1.e = t2.e OR t1.d = t2.b AND t1.f=t2.f OR t1.c = t2.b AND t1.f=t2.f;
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on (or(t1.d = t2.b and t1.e = t2.e, t1.d = t2.b and t1.f=t2.f, t1.c = t2.b and t1.e = t2.e, t1.c = t2.b and t1.f=t2.f));
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on (t1.d = t2.b and t1.e = t2.e or t1.d = t2.b and t1.f=t2.f or t1.c = t2.b and t1.e = t2.e or t1.c = t2.b and t1.f=t2.f);

SELECT 'left ---';

select *
from
    (select '1' as a, 'X' as b, 'Y' as c, 'Z' as d, 'W' as e, 'V1' as f
     union all select '2', 'A', 'B', 'C', 'D', 'E1'
     union all select '3', 'F', 'G', 'H', 'I', 'J1') as t1
left join
    (select '1' as a, 'X' as b, 'P' as c, 'Z' as d, 'W' as e, 'V1' as f
     union all select '2', 'B', 'Q', 'C', 'D', 'E1'
     union all select '4', 'L', 'M', 'N', 'O', 'P1') as t2
on (t1.d = t2.b or t1.c = t2.b or t1.d = t2.b and t1.d = t2.b) or (t1.e = t2.e and t1.a = t2.a and t2.f = t1.f)
ORDER BY ALL
SETTINGS enable_analyzer = 1, query_plan_use_new_logical_join_step = 1;


SELECT 'right ---';
select *
from
    (select '1' as a, 'X' as b, 'Y' as c, 'Z' as d, 'W' as e, 'V1' as f
     union all select '2', 'A', 'B', 'C', 'D', 'E1'
     union all select '3', 'F', 'G', 'H', 'I', 'J1') as t1
right join
    (select '1' as a, 'X' as b, 'P' as c, 'Z' as d, 'W' as e, 'V1' as f
     union all select '2', 'B', 'Q', 'C', 'D', 'E1'
     union all select '4', 'L', 'M', 'N', 'O', 'P1') as t2
on
    (t1.d = t2.b OR t1.c = t2.b) OR t1.e = t2.e
ORDER BY ALL
SETTINGS enable_analyzer = 1, query_plan_use_new_logical_join_step = 1;

SELECT 'inner ---';

select *
from
    (select '1' as a, 'X' as b, 'Y' as c, 'Z' as d, 'W' as e, 'V1' as f
     union all select '2', 'A', 'B', 'C', 'D', 'E1'
     union all select '3', 'F', 'G', 'H', 'I', 'J1') as t1
inner join
    (select '1' as a, 'X' as b, 'P' as c, 'Z' as d, 'W' as e, 'V1' as f
     union all select '2', 'B', 'Q', 'C', 'D', 'E1'
     union all select '4', 'L', 'M', 'N', 'O', 'P1') as t2
on
    (t1.d = t2.b OR t1.c = t2.b) OR (t1.e = t2.e AND t1.a = t2.a AND t2.f = t1.f)
ORDER BY ALL
SETTINGS enable_analyzer = 1, query_plan_use_new_logical_join_step = 1;

select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 left join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on (t1.d = t2.b or t1.c = t2.b or t1.d = t2.b and t1.d = t2.b) or (t1.e = t2.e and t1.a=t2.a and t2.f=t1.f) ORDER BY ALL SETTINGS enable_analyzer = 1, query_plan_use_new_logical_join_step = 0;
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 right join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on (t1.d = t2.b or t1.c = t2.b) or t1.e = t2.e ORDER BY ALL SETTINGS enable_analyzer = 1, query_plan_use_new_logical_join_step = 0;
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on (t1.d = t2.b or t1.c = t2.b) or (t1.e = t2.e and t1.a=t2.a and t2.f=t1.f) ORDER BY ALL SETTINGS enable_analyzer = 1, query_plan_use_new_logical_join_step = 0;
select * from (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t1 inner join (select 'a' as a, 'b' as b, 'c' as c, 'd' as d, 'e' as e, 'f' as f) as t2 on (t1.d = t2.b or t1.c = t2.b) and (t1.e = t2.e or t1.f=t2.f) SETTINGS enable_analyzer = 1;

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
select a2, b2, a3, b3 from tab2 any left join tab3 on a2=a3 and a2 +1 = b3 + 0 or b2=b3 and a2 +1 = b3 + 0 ORDER BY ALL;

drop table tab2;
drop table tab3;

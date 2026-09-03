select '1 left', * from (select 1 as x, 2 as y) t1 left join (select 1 as xx, 2 as yy from numbers(1)) t2  on x = xx or y = yy;

select '5 left', * from (select 1 as x, 2 as y) t1 left join (select 1 as xx, 2 as yy from numbers(5)) t2  on x = xx or y = yy;

select '15 left', * from (select 1 as x, 2 as y) t1 left join (select 1 as xx, 2 as yy from numbers(15)) t2  on x = xx or y = yy;

select '16 left', * from (select 1 as x, 2 as y) t1 left join (select 1 as xx, 2 as yy from numbers(16)) t2  on x = xx or y = yy;

select '17 left', * from (select 1 as x, 2 as y) t1 left join (select 1 as xx, 2 as yy from numbers(17)) t2  on x = xx or y = yy;

select '17 any left', * from (select 1 as x, 2 as y) t1 any left join (select 1 as xx, 2 as yy from numbers(17)) t2  on x = xx or y = yy;

select '17 right', * from (select 1 as x, 2 as y) t1 right join (select 1 as xx, 2 as yy from numbers(17)) t2  on x = xx or y = yy;

select '17 any right', * from (select 1 as x, 2 as y) t1 any right join (select 1 as xx, 2 as yy from numbers(17)) t2  on x = xx or y = yy;

select '17 full', * from (select 1 as x, 2 as y) t1 full join (select 1 as xx, 2 as yy from numbers(17)) t2  on x = xx or y = yy;

select count(1) from (select * from (select 1 as x, 2 as y) t1 left join (select 1 as xx, 2 as yy from numbers(555)) t2  on x = xx or y = yy);

select * from (select 'a' as a, number as c from numbers(2)) as t1 join (select 'a' as a, number as c from numbers(2)) as t2  on  t1.c = t2.c or t1.a = t2.a order by t1.c, t2.c;

select * from (select 'a' as a, number as c from numbers(2)) as t1 join (select 'a' as a, number as c from numbers(2)) as t2  on  t1.a = t2.a or t1.c = t2.c order by t1.c, t2.c;

select * from (select 'a' as a, 'b' as b, 42 as forty_two) as t1 inner join (select 'a' as a, 'b' as b, 42 as forty_two) as t2 on t1.b = t2.a or t1.forty_two = t2.forty_two;
select '===';
select * from (select 'a' as a, 'b' as b, 42 as forty_two) as t1 inner join (select 'a' as a, 'b' as b, 42 as forty_two) as t2 on t1.b = t2.b or t1.forty_two = t2.forty_two;

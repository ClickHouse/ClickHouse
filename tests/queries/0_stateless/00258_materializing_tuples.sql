select * from (select tuple(1) as a union all select tuple(1) as a) order by a;
select * from (select tuple(1) as a union all select tuple(2) as a) order by a;
select * from (select tuple(materialize(0)) as a union all select tuple(0) as a) order by a;
select * from (select tuple(range(1)[1]) as a union all select tuple(0) as a) order by a;
select * from (select tuple(range(1)[2]) as a union all select tuple(1) as a) order by a;

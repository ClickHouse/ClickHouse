-- https://github.com/ClickHouse/ClickHouse/issues/9810
select cast(1 as String)
from (select 1 as iid) as t1
join (select '1' as sid) as t2 on t2.sid = cast(t1.iid as String);

-- even simpler cases
select cast(7 as String), * from (select 3 "'String'");
SELECT concat('xyz', 'abc'), * FROM (SELECT 2 AS "'xyz'");



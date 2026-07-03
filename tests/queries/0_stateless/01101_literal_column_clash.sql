-- https://github.com/ClickHouse/ClickHouse/issues/9810
select cast(1 as String)
from (select 1 as iid) as t1
join (select '1' as sid) as t2 on t2.sid = cast(t1.iid as String);

-- even simpler cases
select cast(7 as String), * from (select 3 "'String'");
select cast(7 as String), * from (select number "'String'" FROM numbers(2));
SELECT concat('xyz', 'abc'), * FROM (SELECT 2 AS "'xyz'");
with 3 as "1" select 1, "1"; -- { serverError AMBIGUOUS_COLUMN_NAME }

-- https://github.com/ClickHouse/ClickHouse/issues/9953
select 1, * from (select 2 x) a left join (select 1, 3 y) b on y = x;
select 1, * from (select 2 x, 1) a right join (select 3 y) b on y = x;
select null, isConstant(null), * from (select 2 x) a left join (select null, 3 y) b on y = x;
select null, isConstant(null), * from (select 2 x, null) a right join (select 3 y) b on y = x;

-- other cases with joins and constants

select cast(1, 'UInt8') from (select arrayJoin([1, 2]) as a) t1 left join (select 1 as b) t2 on b = ignore('UInt8'); -- { serverError INVALID_JOIN_ON_EXPRESSION }

select isConstant('UInt8'), toFixedString('hello', toUInt8(substring('UInt8', 5, 1))) from (select arrayJoin([1, 2]) as a) t1 left join (select 1 as b) t2 on b = ignore('UInt8');  -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- https://github.com/ClickHouse/ClickHouse/issues/20624
select 2 as `toString(x)`, x from (select 1 as x);

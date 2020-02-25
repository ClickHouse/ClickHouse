set join_algorithm = 'hash';

select * from (select number as n from numbers(4)) t1
join (select number as n from numbers(4)) as t2
on t1.n = toUInt64(0)
order by t1.n, t2.n;

select '-';

select * from (select number as n from numbers(4)) t1
join (select number as n from numbers(4)) as t2
on t2.n = toUInt64(0)
order by t1.n, t2.n;

select '-';

set join_algorithm = 'partial_merge';

select * from (select number as n from numbers(4)) t1
join (select number as n from numbers(4)) as t2
on t1.n = toUInt64(0)
order by t1.n, t2.n;

select '-';

select * from (select number as n from numbers(4)) t1
join (select number as n from numbers(4)) as t2
on t2.n = toUInt64(0)
order by t1.n, t2.n;

select '-';

set enable_optimize_predicate_expression = 0;

set join_algorithm = 'hash';

select * from (select number as n from numbers(4)) t1
join (select number as n from numbers(4)) as t2
on t1.n = toUInt64(0)
order by t1.n, t2.n;

select '-';

select * from (select number as n from numbers(4)) t1
join (select number as n from numbers(4)) as t2
on t2.n = toUInt64(0)
order by t1.n, t2.n;

select '-';

set join_algorithm = 'partial_merge';

select * from (select number as n from numbers(4)) t1
join (select number as n from numbers(4)) as t2
on t1.n = toUInt64(0)
order by t1.n, t2.n;

select '-';

select * from (select number as n from numbers(4)) t1
join (select number as n from numbers(4)) as t2
on t2.n = toUInt64(0)
order by t1.n, t2.n;

SET optimize_read_in_order = 1, query_plan_read_in_order = 1, enable_analyzer = 0;

drop table if exists tab;
drop table if exists tab2;
drop table if exists tab3;
drop table if exists tab4;
drop table if exists tab5;

create table tab (a UInt32, b UInt32, c UInt32, d UInt32) engine = MergeTree order by ((a + b) * c, sin(a / b));
insert into tab select number, number, number, number from numbers(5);
insert into tab select number, number, number, number from numbers(5);

-- { echoOn }

-- Exact match, single key
select * from tab order by (a + b) * c;
select * from (explain plan actions = 1 select * from tab order by (a + b) * c) where explain like '%sort description%';

select * from tab order by (a + b) * c desc;
select * from (explain plan actions = 1 select * from tab order by (a + b) * c desc) where explain like '%sort description%';

-- Exact match, full key
select * from tab order by (a + b) * c, sin(a / b);
select * from (explain plan actions = 1 select * from tab order by (a + b) * c, sin(a / b)) where explain like '%sort description%';

select * from tab order by (a + b) * c desc, sin(a / b) desc nulls first;
select * from (explain plan actions = 1 select * from tab order by (a + b) * c desc, sin(a / b) desc nulls first) where explain like '%sort description%';

-- Exact match, mixed direction
select * from tab order by (a + b) * c desc, sin(a / b);
select * from (explain plan actions = 1 select * from tab order by (a + b) * c desc, sin(a / b)) where explain like '%sort description%';

select * from tab order by (a + b) * c, sin(a / b) desc;
select * from (explain plan actions = 1 select * from tab order by (a + b) * c, sin(a / b) desc) where explain like '%sort description%';

-- Wrong order, full sort
select * from tab order by sin(a / b), (a + b) * c;
select * from (explain plan actions = 1 select * from tab order by sin(a / b), (a + b) * c) where explain ilike '%sort description%';

-- Fixed point
select * from tab where (a + b) * c = 8 order by sin(a / b);
select * from (explain plan actions = 1 select * from tab where (a + b) * c = 8 order by sin(a / b)) where explain ilike '%sort description%';

select * from tab where d + 1 = 2 order by (d + 1) * 4, (a + b) * c;
select * from (explain plan actions = 1 select * from tab where d + 1 = 2 order by (d + 1) * 4, (a + b) * c) where explain ilike '%sort description%';

select * from tab where d + 1 = 3 and (a + b) = 4 and c = 2 order by (d + 1) * 4, sin(a / b);
select * from (explain plan actions = 1 select * from tab where d + 1 = 3 and (a + b) = 4 and c = 2 order by (d + 1) * 4, sin(a / b)) where explain ilike '%sort description%';

-- Wrong order with fixed point
select * from tab where (a + b) * c = 8 order by sin(b / a);
select * from (explain plan actions = 1 select * from tab where (a + b) * c = 8 order by sin(b / a)) where explain ilike '%sort description%';

-- Monotonicity
select * from tab order by intDiv((a + b) * c, 2);
select * from (explain plan actions = 1 select * from tab order by intDiv((a + b) * c, 2)) where explain like '%sort description%';

select * from tab order by intDiv((a + b) * c, 2), sin(a / b);
select * from (explain plan actions = 1 select * from tab order by intDiv((a + b) * c, 2), sin(a / b)) where explain like '%sort description%';

-- select * from tab order by (a + b) * c, intDiv(sin(a / b), 2);
select * from (explain plan actions = 1 select * from tab order by (a + b) * c, intDiv(sin(a / b), 2)) where explain like '%sort description%';

-- select * from tab order by (a + b) * c desc , intDiv(sin(a / b), 2);
select * from (explain plan actions = 1 select * from tab order by (a + b) * c desc , intDiv(sin(a / b), 2)) where explain like '%sort description%';

-- select * from tab order by (a + b) * c, intDiv(sin(a / b), 2) desc;
select * from (explain plan actions = 1 select * from tab order by (a + b) * c, intDiv(sin(a / b), 2) desc) where explain like '%sort description%';

-- select * from tab order by (a + b) * c desc, intDiv(sin(a / b), 2) desc;
select * from (explain plan actions = 1 select * from tab order by (a + b) * c desc, intDiv(sin(a / b), 2) desc nulls first) where explain like '%sort description%';

-- select * from tab order by (a + b) * c desc, intDiv(sin(a / b), -2);
select * from (explain plan actions = 1 select * from tab order by (a + b) * c desc, intDiv(sin(a / b), -2)) where explain like '%sort description%';

-- select * from tab order by (a + b) * c desc, intDiv(intDiv(sin(a / b), -2), -3);
select * from (explain plan actions = 1 select * from tab order by (a + b) * c desc, intDiv(intDiv(sin(a / b), -2), -3)) where explain like '%sort description%';

-- select * from tab order by (a + b) * c, intDiv(intDiv(sin(a / b), -2), -3);
select * from (explain plan actions = 1 select * from tab order by (a + b) * c, intDiv(intDiv(sin(a / b), -2), -3)) where explain like '%sort description%';

-- Aliases
select * from (select *, a + b as x from tab) order by x * c;
select * from (explain plan actions = 1 select * from (select *, a + b as x from tab) order by x * c) where explain like '%sort description%';

select * from (select *, a + b as x, a / b as y from tab) order by x * c, sin(y);
select * from (explain plan actions = 1 select * from (select *, a + b as x, a / b as y from tab) order by x * c, sin(y)) where explain like '%sort description%';

select * from (select *, a / b as y from (select *, a + b as x from tab)) order by x * c, sin(y);
select * from (explain plan actions = 1 select * from (select *, a / b as y from (select *, a + b as x from tab)) order by x * c, sin(y)) where explain like '%sort description%';

-- { echoOff }

create table tab2 (x DateTime, y UInt32, z UInt32) engine = MergeTree order by (x, y);
insert into tab2 select toDate('2020-02-02') + number, number, number from numbers(4);
insert into tab2 select toDate('2020-02-02') + number, number, number from numbers(4);

-- { echoOn }

select * from tab2 order by toTimeZone(toTimezone(x, 'UTC'), 'CET'), intDiv(intDiv(y, -2), -3);
select * from (explain plan actions = 1 select * from tab2 order by toTimeZone(toTimezone(x, 'UTC'), 'CET'), intDiv(intDiv(y, -2), -3)) where explain like '%sort description%';

select * from tab2 order by toStartOfDay(x), intDiv(intDiv(y, -2), -3);
select * from (explain plan actions = 1 select * from tab2 order by toStartOfDay(x), intDiv(intDiv(y, -2), -3)) where explain like '%sort description%';

-- select * from tab2 where toTimezone(x, 'CET') = '2020-02-03 01:00:00' order by intDiv(intDiv(y, -2), -3);
select * from (explain plan actions = 1 select * from tab2 where toTimezone(x, 'CET') = '2020-02-03 01:00:00' order by intDiv(intDiv(y, -2), -3)) where explain like '%sort description%';

-- { echoOff }

create table tab3 (a UInt32, b UInt32, c UInt32, d UInt32) engine = MergeTree order by ((a + b) * c, sin(a / b));
insert into tab3 select number, number, number, number from numbers(5);
insert into tab3 select number, number, number, number from numbers(5);

create table tab4 (a UInt32, b UInt32, c UInt32, d UInt32) engine = MergeTree order by sin(a / b);
insert into tab4 select number, number, number, number from numbers(5);
insert into tab4 select number, number, number, number from numbers(5);

create table tab5 (a UInt32, b UInt32, c UInt32, d UInt32) engine = MergeTree order by (a + b) * c;
insert into tab5 select number, number, number, number from numbers(5);
insert into tab5 select number, number, number, number from numbers(5);

-- { echoOn }

-- Union (not fully supported)
select * from (select * from tab union all select * from tab3) order by (a + b) * c, sin(a / b);
select * from (explain plan actions = 1 select * from (select * from tab union all select * from tab3) order by (a + b) * c, sin(a / b)) where explain like '%sort description%' or explain like '%ReadType%';

select * from (select * from tab where (a + b) * c = 8 union all select * from tab3 where (a + b) * c = 18) order by sin(a / b);
select * from (explain plan actions = 1 select * from (select * from tab where (a + b) * c = 8 union all select * from tab3 where (a + b) * c = 18) order by sin(a / b)) where explain like '%sort description%' or explain like '%ReadType%';

select * from (select * from tab where (a + b) * c = 8 union all select * from tab4) order by sin(a / b);
select * from (explain plan actions = 1 select * from (select * from tab where (a + b) * c = 8 union all select * from tab4) order by sin(a / b)) where explain like '%sort description%' or explain like '%ReadType%';

select * from (select * from tab union all select * from tab5) order by (a + b) * c;
select * from (explain plan actions = 1 select * from (select * from tab union all select * from tab5) order by (a + b) * c) where explain like '%sort description%' or explain like '%ReadType%';

select * from (select * from tab union all select * from tab5) order by (a + b) * c, sin(a / b);
select * from (explain plan actions = 1 select * from (select * from tab union all select * from tab5) order by (a + b) * c, sin(a / b)) where explain like '%sort description%' or explain like '%ReadType%';

-- Union with limit
select * from (select * from tab union all select * from tab5) order by (a + b) * c, sin(a / b) limit 3;
select * from (explain plan actions = 1 select * from (select * from tab union all select * from tab5) order by (a + b) * c, sin(a / b) limit 3) where explain ilike '%sort description%' or explain like '%ReadType%' or explain like '%Limit%';

-- In this example, we read-in-order from tab up to ((a + b) * c, sin(a / b)) and from tab5 up to ((a + b) * c).
-- In case of tab5, there would be two finish sorting transforms: ((a + b) * c) -> ((a + b) * c, sin(a / b)) -> ((a + b) * c, sin(a / b), d).
-- It's important that ((a + b) * c) -> ((a + b) * c does not have LIMIT. We can add LIMIT WITH TIES later, when sorting alog support it.
-- In case of tab4, we do full sorting by ((a + b) * c, sin(a / b), d) with LIMIT. We can replace it to sorting by ((a + b) * c, sin(a / b)) and LIMIT WITH TIES, when sorting alog support it.
select * from (select * from tab union all select * from tab5 union all select * from tab4) order by (a + b) * c, sin(a / b), d limit 3;
select * from (explain plan actions = 1 select * from (select * from tab union all select * from tab5 union all select * from tab4) order by (a + b) * c, sin(a / b), d limit 3) where explain ilike '%sort description%' or explain like '%ReadType%' or explain like '%Limit%';

drop table if exists tab;
drop table if exists tab2;
drop table if exists tab3;
drop table if exists tab4;
drop table if exists tab5;

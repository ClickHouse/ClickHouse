select toTypeName(makeDate(1991, 8, 24));
select toTypeName(makeDate(cast(1991 as Nullable(UInt64)), 8, 24));
select toTypeName(makeDate(1991, cast(8 as Nullable(UInt64)), 24));
select toTypeName(makeDate(1991, 8, cast(24 as Nullable(UInt64))));
select toTypeName(makeDate(1991, cast(8 as Nullable(UInt64)), cast(24 as Nullable(UInt64))));

select makeDate(1970, 01, 01);
select makeDate(2020, 08, 24);
select makeDate(1980, 10, 17);
select makeDate(-1980, 10, 17);
select makeDate(1980, -10, 17);
select makeDate(1980, 10, -17);
select makeDate(1980.0, '10', 30.0/2);
select makeDate(-1980.0, '10', 30.0/2);
select makeDate(cast(1980.1 as Decimal(20,5)), '10', 30.0/2);
select makeDate(cast('-1980.1' as Decimal(20,5)), '10', 30.0/2);
select makeDate(cast(1980.1 as Float32), '10', 30.0/2);
select makeDate(cast(-1980.1 as Float32), '10', 30.0/2);
--select makeDate(cast(1980 as Date), '10', 30.0/2);
--select makeDate(cast(-1980 as Date), '10', 30.0/2);
--select makeDate(cast(1980 as Date32), '10', 30.0/2);
--select makeDate(cast(-1980 as Date32), '10', 30.0/2);
--select makeDate(cast(1980 as DateTime), '10', 30.0/2);
--select makeDate(cast(-1980 as DateTime), '10', 30.0/2);
--select makeDate(cast(1980 as DateTime64), '10', 30.0/2);
--select makeDate(cast(-1980 as DateTime64), '10', 30.0/2);

select makeDate(0.0, 1, 2);
select makeDate(1980, 15, 1);
select makeDate(1980, 2, 29);
select makeDate(1984, 2, 30);
select makeDate(19800, 12, 3);
select makeDate(2148,1,1);
select makeDate(2149,1,1);
select makeDate(2149,6,6);
select makeDate(2149,6,7);
select makeDate(2150,1,1);
select makeDate(1969,1,1);
select makeDate(1969,12,1);
select makeDate(1969,12,31);
select makeDate(1970,0,0);
select makeDate(1970,0,1);
select makeDate(1970,1,0);
select makeDate(1990,0,1);
select makeDate(1990,1,0);

select makeDate('1980', '10', '20');
select makeDate('-1980', 3, 17);

select makeDate('aa', 3, 24);
select makeDate(1994, 'aa', 24);
select makeDate(1984, 3, 'aa');

select makeDate(True, 3, 24);
select makeDate(1994, True, 24);
select makeDate(1984, 3, True);
select makeDate(False, 3, 24);
select makeDate(1994, False, 24);
select makeDate(1984, 3, False);

select makeDate(NULL, 3, 4);
select makeDate(1980, NULL, 4);
select makeDate(1980, 3, NULL);

select makeDate(year, month, day) from (select NULL as year, 2 as month, 3 as day union all select 1984 as year, 2 as month, 3 as day) order by year, month, day;

select makeDate(year, month, day) from (select NULL as year, 2 as month, 3 as day union all select NULL as year, 2 as month, 3 as day) order by year, month, day;

select makeDate(year, month, day) from (select 1984 as year, 2 as month, 3 as day union all select 1984 as year, 2 as month, 4 as day) order by year, month, day;

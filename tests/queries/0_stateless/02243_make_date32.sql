select toTypeName(makeDate32(1991, 8, 24));
select toTypeName(makeDate32(cast(1991 as Nullable(UInt64)), 8, 24));
select toTypeName(makeDate32(1991, cast(8 as Nullable(UInt64)), 24));
select toTypeName(makeDate32(1991, 8, cast(24 as Nullable(UInt64))));
select toTypeName(makeDate32(1991, cast(8 as Nullable(UInt64)), cast(24 as Nullable(UInt64))));

select makeDate32(1970, 01, 01);
select makeDate32(2020, 08, 24);
select makeDate32(1980, 10, 17);
select makeDate32(-1980, 10, 17);
select makeDate32(1980, -10, 17);
select makeDate32(1980, 10, -17);
select makeDate32(1980.0, 9, 30.0/2);
select makeDate32(-1980.0, 9, 32.0/2);
select makeDate32(cast(1980.1 as Decimal(20,5)), 9, 17);
select makeDate32(cast('-1980.1' as Decimal(20,5)), 9, 18);
select makeDate32(cast(1980.1 as Float32), 9, 19);
select makeDate32(cast(-1980.1 as Float32), 9, 20);

select makeDate32(cast(1980 as Date), 10, 30); -- { serverError 43 }
select makeDate32(cast(-1980 as Date), 10, 30); -- { serverError 43 }
select makeDate32(cast(1980 as Date32), 10, 30); -- { serverError 43 }
select makeDate32(cast(-1980 as Date32), 10, 30); -- { serverError 43 }
select makeDate32(cast(1980 as DateTime), 10, 30); -- { serverError 43 }
select makeDate32(cast(-1980 as DateTime), 10, 30); -- { serverError 43 }
select makeDate32(cast(1980 as DateTime64), 10, 30); -- { serverError 43 }
select makeDate32(cast(-1980 as DateTime64), 10, 30); -- { serverError 43 }

select makeDate32(0.0, 1, 2);
select makeDate32(1980, 15, 1);
select makeDate32(1980, 2, 29);
select makeDate32(1984, 2, 30);
select makeDate32(19800, 12, 3);
select makeDate32(2148,1,1);
select makeDate32(2149,1,1);
select makeDate32(2149,6,6);
select makeDate32(2149,6,7);
select makeDate32(2150,1,1);
select makeDate32(1969,1,1);
select makeDate32(1969,12,1);
select makeDate32(1969,12,31);
select makeDate32(2282,1,1);
select makeDate32(2283,1,1);
select makeDate32(2283,11,11);
select makeDate32(2283,11,12);
select makeDate32(2284,1,1);
select makeDate32(1924,1,1);
select makeDate32(1924,12,1);
select makeDate32(1924,12,31);
select makeDate32(1970,0,0);
select makeDate32(1970,0,1);
select makeDate32(1970,1,0);
select makeDate32(1990,0,1);
select makeDate32(1990,1,0);

select makeDate32(0x7fff+2010,1,1);
select makeDate32(0xffff+2010,1,2);
select makeDate32(0x7fffffff+2010,1,3);
select makeDate32(0xffffffff+2010,1,4);
select makeDate32(0x7fffffffffffffff+2010,1,3);
select makeDate32(0xffffffffffffffff+2010,1,4);

select makeDate32('1980', '10', '20'); -- { serverError 43 }
select makeDate32('-1980', 3, 17); -- { serverError 43 }

select makeDate32('aa', 3, 24); -- { serverError 43 }
select makeDate32(1994, 'aa', 24); -- { serverError 43 }
select makeDate32(1984, 3, 'aa'); -- { serverError 43 }

select makeDate32(True, 3, 24);
select makeDate32(1994, True, 24);
select makeDate32(1984, 3, True);
select makeDate32(False, 3, 24);
select makeDate32(1994, False, 24);
select makeDate32(1984, 3, False);

select makeDate32(NULL, 3, 4);
select makeDate32(1980, NULL, 4);
select makeDate32(1980, 3, NULL);

select makeDate32(1980); -- { serverError 42 }
select makeDate32(1980, 1); -- { serverError 42 }
select makeDate32(1980, 1, 1, 1); -- { serverError 42 }

select makeDate32(year, month, day) from (select NULL as year, 2 as month, 3 as day union all select 1984 as year, 2 as month, 3 as day) order by year, month, day;

select makeDate32(year, month, day) from (select NULL as year, 2 as month, 3 as day union all select NULL as year, 2 as month, 3 as day) order by year, month, day;

select makeDate32(year, month, day) from (select 1984 as year, 2 as month, 3 as day union all select 1984 as year, 2 as month, 4 as day) order by year, month, day;

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
select makeDate(1980.0, 9, 30.0/2);
select makeDate(-1980.0, 9, 32.0/2);
select makeDate(cast(1980.1 as Decimal(20,5)), 9, 17);
select makeDate(cast('-1980.1' as Decimal(20,5)), 9, 18);
select makeDate(cast(1980.1 as Float32), 9, 19);
select makeDate(cast(-1980.1 as Float32), 9, 20);

select makeDate(cast(1980 as Date), 10, 30); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(-1980 as Date), 10, 30); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(1980 as Date32), 10, 30); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(-1980 as Date32), 10, 30); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(1980 as DateTime), 10, 30); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(-1980 as DateTime), 10, 30); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(1980 as DateTime64), 10, 30); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(-1980 as DateTime64), 10, 30); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

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
select makeDate(2282,1,1);
select makeDate(2283,1,1);
select makeDate(2283,11,11);
select makeDate(2283,11,12);
select makeDate(2284,1,1);
select makeDate(1924,1,1);
select makeDate(1924,12,1);
select makeDate(1924,12,31);
select makeDate(1970,0,0);
select makeDate(1970,0,1);
select makeDate(1970,1,0);
select makeDate(1990,0,1);
select makeDate(1990,1,0);

select makeDate(0x7fff+2010,1,1);
select makeDate(0xffff+2010,1,2);
select makeDate(0x7fffffff+2010,1,3);
select makeDate(0xffffffff+2010,1,4);
select makeDate(0x7fffffffffffffff+2010,1,3);
select makeDate(0xffffffffffffffff+2010,1,4);

select makeDate('1980', '10', '20'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate('-1980', 3, 17); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

select makeDate('aa', 3, 24); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(1994, 'aa', 24); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(1984, 3, 'aa'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

select makeDate(True, 3, 24);
select makeDate(1994, True, 24);
select makeDate(1984, 3, True);
select makeDate(False, 3, 24);
select makeDate(1994, False, 24);
select makeDate(1984, 3, False);

select makeDate(NULL, 3, 4);
select makeDate(1980, NULL, 4);
select makeDate(1980, 3, NULL);

select makeDate(1980); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select makeDate(1980, 1, 1, 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

select MAKEDATE(1980, 1, 1);
select MAKEDATE(1980, 1);

select makeDate(year, month, day) from (select NULL as year, 2 as month, 3 as day union all select 1984 as year, 2 as month, 3 as day) order by year, month, day;

select makeDate(year, month, day) from (select NULL as year, 2 as month, 3 as day union all select NULL as year, 2 as month, 3 as day) order by year, month, day;

select makeDate(year, month, day) from (select 1984 as year, 2 as month, 3 as day union all select 1984 as year, 2 as month, 4 as day) order by year, month, day;

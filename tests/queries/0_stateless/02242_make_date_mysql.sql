select toTypeName(makeDate(1991, 8));
select toTypeName(makeDate(cast(1991 as Nullable(UInt64)), 8));
select toTypeName(makeDate(1991, cast(8 as Nullable(UInt64))));

select makeDate(1970, 01);
select makeDate(2020, 08);
select makeDate(-1980, 10);
select makeDate(1980, -10);
select makeDate(1980.0, 9);
select makeDate(-1980.0, 9);
select makeDate(cast(1980.1 as Decimal(20,5)), 9);
select makeDate(cast('-1980.1' as Decimal(20,5)), 9);
select makeDate(cast(1980.1 as Float32), 9);
select makeDate(cast(-1980.1 as Float32), 9);

select makeDate(cast(1980 as Date), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(-1980 as Date), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(1980 as Date32), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(-1980 as Date32), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(1980 as DateTime), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(-1980 as DateTime), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(1980 as DateTime64), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(cast(-1980 as DateTime64), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate('1980', '10'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate('-1980', 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate('aa', 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate(1994, 'aa'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

select makeDate(0, 1);
select makeDate(19800, 12);
select makeDate(2149, 157);
select makeDate(2149, 158);
select makeDate(1969,355);
select makeDate(1969,356);
select makeDate(1969,357);
select makeDate(1970,0);
select makeDate(1970,1);
select makeDate(1970,2);

select makeDate(NULL, 3);
select makeDate(1980, NULL);

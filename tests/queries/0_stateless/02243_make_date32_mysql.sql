select toTypeName(makeDate32(1991, 8));
select toTypeName(makeDate32(cast(1991 as Nullable(UInt64)), 8));
select toTypeName(makeDate32(1991, cast(8 as Nullable(UInt64))));

select makeDate32(1900, 01);
select makeDate32(2020, 08);
select makeDate32(-1980, 10);
select makeDate32(1980, -10);
select makeDate32(1980.0, 9);
select makeDate32(-1980.0, 9);
select makeDate32(cast(1980.1 as Decimal(20,5)), 9);
select makeDate32(cast('-1980.1' as Decimal(20,5)), 9);
select makeDate32(cast(1980.1 as Float32), 9);
select makeDate32(cast(-1980.1 as Float32), 9);

select makeDate32(cast(1980 as Date), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate32(cast(-1980 as Date), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate32(cast(1980 as Date32), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate32(cast(-1980 as Date32), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate32(cast(1980 as DateTime), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate32(cast(-1980 as DateTime), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate32(cast(1980 as DateTime64), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate32(cast(-1980 as DateTime64), 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate32('1980', '10'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate32('-1980', 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate32('aa', 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select makeDate32(1994, 'aa'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

select makeDate32(0, 1);
select makeDate32(19800, 12);
select makeDate32(2299, 365);
select makeDate32(2299, 366);
select makeDate32(2300, 1);
select makeDate32(1899, 365);
select makeDate32(1899, 366);
select makeDate32(1899, 367);
select makeDate32(1900, 0);
select makeDate32(1900, 1);
select makeDate32(1900, 2);

select makeDate32(NULL, 3);
select makeDate32(1980, NULL);

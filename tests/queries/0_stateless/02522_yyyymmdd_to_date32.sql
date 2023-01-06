select toTypeName(YYYYMMDDToDate32(19910824));
select toTypeName(YYYYMMDDToDate32(cast(19910824 as Nullable(UInt64))));

select YYYYMMDDToDate32(19700101);
select YYYYMMDDToDate32(20200824);
select YYYYMMDDToDate32(19801017);
select YYYYMMDDToDate32(-19801017);
select YYYYMMDDToDate32(19800930.05);
select YYYYMMDDToDate32(-1980/2);
select YYYYMMDDToDate32(cast(19800202.1 as Decimal(20,5)));
select YYYYMMDDToDate32(cast('-19800202.1' as Decimal(20,5)));
select YYYYMMDDToDate32(cast(19800919.1 as Float32));
select YYYYMMDDToDate32(cast(-19800929.1 as Float32));

select YYYYMMDDToDate32(cast(19801030 as Date)); -- { serverError 43 }
select YYYYMMDDToDate32(cast(-19801030 as Date)); -- { serverError 43 }
select YYYYMMDDToDate32(cast(19801030 as Date32)); -- { serverError 43 }
select YYYYMMDDToDate32(cast(-19801030 as Date32)); -- { serverError 43 }
select YYYYMMDDToDate32(cast(19801030 as DateTime)); -- { serverError 43 }
select YYYYMMDDToDate32(cast(-19801030 as DateTime)); -- { serverError 43 }
select YYYYMMDDToDate32(cast(19801030 as DateTime64)); -- { serverError 43 }
select YYYYMMDDToDate32(cast(-19801030 as DateTime64)); -- { serverError 43 }

select YYYYMMDDToDate32(0130.0);
select YYYYMMDDToDate32(19801501);
select YYYYMMDDToDate32(19800229);
select YYYYMMDDToDate32(19840230);
select YYYYMMDDToDate32(198001203);
select YYYYMMDDToDate32(21480101);
select YYYYMMDDToDate32(21490101);
select YYYYMMDDToDate32(21490606);
select YYYYMMDDToDate32(21490607);
select YYYYMMDDToDate32(21500101);
select YYYYMMDDToDate32(19690101);
select YYYYMMDDToDate32(19691201);
select YYYYMMDDToDate32(19691231);
select YYYYMMDDToDate32(22820101);
select YYYYMMDDToDate32(22830101);
select YYYYMMDDToDate32(22831111);
select YYYYMMDDToDate32(22831112);
select YYYYMMDDToDate32(22840101);
select YYYYMMDDToDate32(19240101);
select YYYYMMDDToDate32(19241201);
select YYYYMMDDToDate32(19241231);
select YYYYMMDDToDate32(19700000);
select YYYYMMDDToDate32(19700001);
select YYYYMMDDToDate32(19700100);
select YYYYMMDDToDate32(19900001);
select YYYYMMDDToDate32(19900100);

select YYYYMMDDToDate32(0xffffffffffffffff+2010);

select YYYYMMDDToDate32('19801020'); -- { serverError 43 }
select YYYYMMDDToDate32('-19800317'); -- { serverError 43 }
select YYYYMMDDToDate32('aaaaaa'); -- { serverError 43 }

select YYYYMMDDToDate32(True);
select YYYYMMDDToDate32(False);

select YYYYMMDDToDate32(NULL);

select YYYYMMDDToDate32(1980, 1); -- { serverError 42 }

select YYYYMMDDToDate32(YYYYMMDD) from (select 0203 as YYYYMMDD union all select 19840203 as YYYYMMDD) order by YYYYMMDD;

select YYYYMMDDToDate32(YYYYMMDD) from (select 19840203 as YYYYMMDD union all select 19840204 as YYYYMMDD) order by YYYYMMDD;

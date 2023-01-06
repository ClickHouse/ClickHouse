select toTypeName(YYYYMMDDToDate(19910824));
select toTypeName(YYYYMMDDToDate(cast(19910824 as Nullable(UInt64))));

select YYYYMMDDToDate(19700101);
select YYYYMMDDToDate(20200824);
select YYYYMMDDToDate(19801017);
select YYYYMMDDToDate(-19801017);
select YYYYMMDDToDate(19800930.05);
select YYYYMMDDToDate(-1980/2);
select YYYYMMDDToDate(cast(19800202.1 as Decimal(20,5)));
select YYYYMMDDToDate(cast('-19800202.1' as Decimal(20,5)));
select YYYYMMDDToDate(cast(19800919.1 as Float32));
select YYYYMMDDToDate(cast(-19800929.1 as Float32));

select YYYYMMDDToDate(cast(19801030 as Date)); -- { serverError 43 }
select YYYYMMDDToDate(cast(-19801030 as Date)); -- { serverError 43 }
select YYYYMMDDToDate(cast(19801030 as Date32)); -- { serverError 43 }
select YYYYMMDDToDate(cast(-19801030 as Date32)); -- { serverError 43 }
select YYYYMMDDToDate(cast(19801030 as DateTime)); -- { serverError 43 }
select YYYYMMDDToDate(cast(-19801030 as DateTime)); -- { serverError 43 }
select YYYYMMDDToDate(cast(19801030 as DateTime64)); -- { serverError 43 }
select YYYYMMDDToDate(cast(-19801030 as DateTime64)); -- { serverError 43 }

select YYYYMMDDToDate(0130.0);
select YYYYMMDDToDate(19801501);
select YYYYMMDDToDate(19800229);
select YYYYMMDDToDate(19840230);
select YYYYMMDDToDate(198001203);
select YYYYMMDDToDate(21480101);
select YYYYMMDDToDate(21490101);
select YYYYMMDDToDate(21490606);
select YYYYMMDDToDate(21490607);
select YYYYMMDDToDate(21500101);
select YYYYMMDDToDate(19690101);
select YYYYMMDDToDate(19691201);
select YYYYMMDDToDate(19691231);
select YYYYMMDDToDate(22820101);
select YYYYMMDDToDate(22830101);
select YYYYMMDDToDate(22831111);
select YYYYMMDDToDate(22831112);
select YYYYMMDDToDate(22840101);
select YYYYMMDDToDate(19240101);
select YYYYMMDDToDate(19241201);
select YYYYMMDDToDate(19241231);
select YYYYMMDDToDate(19700000);
select YYYYMMDDToDate(19700001);
select YYYYMMDDToDate(19700100);
select YYYYMMDDToDate(19900001);
select YYYYMMDDToDate(19900100);

select YYYYMMDDToDate(0xffffffffffffffff+2010);

select YYYYMMDDToDate('19801020'); -- { serverError 43 }
select YYYYMMDDToDate('-19800317'); -- { serverError 43 }
select YYYYMMDDToDate('aaaaaa'); -- { serverError 43 }

select YYYYMMDDToDate(True);
select YYYYMMDDToDate(False);

select YYYYMMDDToDate(NULL);

select YYYYMMDDToDate(1980, 1); -- { serverError 42 }

select YYYYMMDDToDate(YYYYMMDD) from (select 0203 as YYYYMMDD union all select 19840203 as YYYYMMDD) order by YYYYMMDD;

select YYYYMMDDToDate(YYYYMMDD) from (select 19840203 as YYYYMMDD union all select 19840204 as YYYYMMDD) order by YYYYMMDD;

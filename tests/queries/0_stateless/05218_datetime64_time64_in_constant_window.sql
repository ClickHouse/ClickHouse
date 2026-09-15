-- `convertFieldToType` is the conversion behind the constants of `IN`. A `Decimal64` or integer constant that no
-- `DateTime64` / `Time64` of the target scale can hold - its rescale overflows the `Int64` ticks, or it lands
-- outside the calendar / clock window - is "cannot convert" (Null) and is excluded from the set, the same way an
-- impossible `Date32` constant is. The query must not fail with `DECIMAL_OVERFLOW`.

select 'DateTime64: impossible constants are excluded';
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 6, 'UTC') in (253402207200000::Decimal64(0)));
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (99999999999999999));
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 0, 'UTC') in (18446744073709551615));
-- Inside the `Int64` ticks at scale 0, yet past the last calendar second `9999-12-31 23:59:59`.
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 0, 'UTC') in (999999999999));
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 0, 'UTC') in (-999999999999));

select 'DateTime64: representable constants still match';
select 1 where toDateTime64('1970-01-01 00:00:01', 6, 'UTC') in (1::Decimal64(0), 253402207200000::Decimal64(0));
select 1 where toDateTime64('1970-01-01 00:00:01.5', 6, 'UTC') in (toDecimal64('1.5', 1));
select 1 where toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (1);
select 1 where toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (toUInt64(1));
select 1 where toDateTime64('9999-12-31 23:59:59', 3, 'UTC') in (253402300799);
select 1 where toDateTime64('1900-01-01 00:00:00', 3, 'UTC') in (-2208988800);
-- The last whole second the `Int64` ticks hold at scale 9.
select 1 where toDateTime64(9223372036, 9, 'UTC') in (9223372036);

select 'DateTime64: a constant that loses its fraction cannot equal a stored value';
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 0, 'UTC') in (toDecimal64('1.9', 1)));
select 1 where toDateTime64('1970-01-01 00:00:01', 0, 'UTC') in (toDecimal64('1.0', 1));

select 'Time64: impossible constants are excluded';
select count() from (select 1 where toTime64('00:00:01', 6) in (253402207200000::Decimal64(0)));
select count() from (select 1 where toTime64('00:00:01', 6) in (-253402207200000::Decimal64(0)));
-- Inside the `Int64` ticks, yet past the clock window of `999:59:59.999999`.
select count() from (select 1 where toTime64('00:00:01', 6) in (9223372036854::Decimal64(0)));
select count() from (select 1 where toTime64('999:59:59', 6) in (3600000));
select count() from (select 1 where toTime64('-999:59:59', 6) in (-3600000));

select 'Time64: representable constants still match';
select 1 where toTime64('00:00:01', 6) in (1::Decimal64(0), 253402207200000::Decimal64(0));
select 1 where toTime64('999:59:59', 6) in (3599999);
select 1 where toTime64('-999:59:59', 6) in (-3599999);
select 1 where toTime64('-00:00:12', 6) in (-12::Decimal64(0));
select 1 where toTime64('00:00:01.5', 6) in (toDecimal64('1.5', 1));
select 1 where toTime64('00:00:01', 0) in (toDecimal64('1.0', 1));
select count() from (select 1 where toTime64('00:00:01', 0) in (toDecimal64('1.9', 1)));

select 'Nullable and multi-element sets';
select toNullable(toDateTime64('1970-01-01 00:00:01', 3, 'UTC')) in (1, 99999999999999999);
select toNullable(toTime64('00:00:01', 3)) in (253402207200000::Decimal64(0), 1);
select toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (99999999999999999, -99999999999999999);

select 'Primary key pruning with an impossible constant';
drop table if exists t_05218;
create table t_05218 (dt DateTime64(3, 'UTC'), t Time64(6)) engine = MergeTree order by (dt, t);
insert into t_05218 values ('1970-01-01 00:00:01.000', '00:00:01.000000'), ('2000-01-01 00:00:00.000', '999:59:59.000000');
select count() from t_05218 where dt in (99999999999999999);
select count() from t_05218 where dt in (1, 99999999999999999);
select count() from t_05218 where t in (253402207200000::Decimal64(0));
select count() from t_05218 where t in (3599999, 253402207200000::Decimal64(0));
select count() from t_05218 where (dt, t) in ((1, 1), (946684800, 253402207200000::Decimal64(0)));
drop table t_05218;

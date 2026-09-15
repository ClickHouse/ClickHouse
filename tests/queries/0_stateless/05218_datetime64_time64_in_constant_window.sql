-- `convertFieldToType` is the conversion behind the constants of `IN`. A `Decimal` or integer constant that no
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

select 'Wide integers and every Decimal width are carriers too';
-- The same constants `CAST` / `toDateTime64` accept match on the `IN` path, whatever the width of the carrier.
select 1 where toDateTime64(1735689600, 3, 'UTC') in (toUInt128(1735689600));
select 1 where toDateTime64(1735689600, 3, 'UTC') in (toInt128(1735689600));
select 1 where toDateTime64(1735689600, 3, 'UTC') in (toUInt256(1735689600));
select 1 where toDateTime64(1735689600, 3, 'UTC') in (toInt256(1735689600));
select 1 where toDateTime64('1900-01-01 00:00:00', 3, 'UTC') in (toInt128(-2208988800));
select 1 where toDateTime64('1970-01-01 00:00:01.5', 3, 'UTC') in (toDecimal32('1.5', 1));
select 1 where toDateTime64('1970-01-01 00:00:01.5', 3, 'UTC') in (toDecimal128('1.5', 1));
select 1 where toDateTime64('1970-01-01 00:00:01.5', 3, 'UTC') in (toDecimal256('1.5', 1));
select 1 where toDateTime64('1970-01-01 00:00:00.5', 3, 'UTC') in (toDecimal32('0.500000000', 9));
select 1 where toDateTime64('1970-01-01 00:00:01.5', 3, 'UTC') in (toDecimal256('1.5', 70));
select 1 where toTime64('00:00:01', 6) in (toUInt128(1));
select 1 where toTime64('-999:59:59', 6) in (toInt256(-3599999));
select 1 where toTime64('00:00:01.5', 6) in (toDecimal32('1.5', 1));
select 1 where toTime64('00:00:01.5', 6) in (toDecimal128('1.5', 30));
-- A constant outside the `Int64` ticks or the calendar / clock window is excluded, not `TYPE_MISMATCH` or `DECIMAL_OVERFLOW`.
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (toUInt128('99999999999999999999')));
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (toInt256('-99999999999999999999')));
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (toUInt128(999999999999)));
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (toDecimal128('999999999999', 0)));
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (toDecimal256('99999999999999999999999999', 0)));
select count() from (select 1 where toTime64('00:00:01', 6) in (toDecimal32('3600000', 0)));
select count() from (select 1 where toTime64('00:00:01', 6) in (toUInt256(3600000)));
select count() from (select 1 where toTime64('00:00:01', 6) in (toInt128(-3600000)));
select count() from (select 1 where toTime64('00:00:01', 6) in (toDecimal128('-3600000', 0)));
-- A constant that loses its fraction cannot equal a stored value, whatever its width.
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 0, 'UTC') in (toDecimal32('1.9', 1)));
select count() from (select 1 where toTime64('00:00:01', 0) in (toDecimal256('1.9', 1)));
select 1 where toDateTime64('1970-01-01 00:00:01', 0, 'UTC') in (toDecimal128('1.0', 1));
-- Mixed sets and Nullable arguments take the same path.
select toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (toUInt128('99999999999999999999'), 1);
select toNullable(toTime64('00:00:01', 3)) in (toDecimal256('99999999999999999999999999', 0), toDecimal256('1', 0));

select 'A Float64 literal is a carrier too';
-- `1.5` is a `Float64` `Field`. It matches on the `IN` path when it is exactly representable at the scale, like its `CAST`.
select 1 where toDateTime64('1970-01-01 00:00:01.5', 1, 'UTC') in (1.5);
select 1 where toDateTime64('1970-01-01 00:00:01.25', 3, 'UTC') in (1.25);
select 1 where toDateTime64(1735689600, 3, 'UTC') in (1735689600.0);
select 1 where toDateTime64('1900-01-01 00:00:00', 3, 'UTC') in (-2208988800.0);
select 1 where toTime64('00:00:01.5', 1) in (1.5);
select 1 where toTime64('-00:00:01.5', 6) in (-1.5);
select 1 where toTime64('-999:59:59.5', 1) in (-3599999.5);
-- A lossy `Float64` (the ticks do not read back as the literal) cannot equal a stored value: `IN (1.25)` at scale 1 is 0,
-- the same rule as `CAST('33.3', 'Decimal64(1)') IN (33.33)`.
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01.2', 1, 'UTC') in (1.25));
select count() from (select 1 where toTime64('00:00:01.2', 1) in (1.25));
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 0, 'UTC') in (1.5));
-- Outside the `Int64` ticks or the calendar / clock window, or not finite: excluded, not `TYPE_MISMATCH` or `DECIMAL_OVERFLOW`.
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (1e30));
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (-1e30));
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (253402207200.5));
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (nan));
select count() from (select 1 where toDateTime64('1970-01-01 00:00:01', 3, 'UTC') in (inf));
select count() from (select 1 where toTime64('00:00:01', 6) in (3600000.5));
select count() from (select 1 where toTime64('00:00:01', 6) in (-3600000.5));
select count() from (select 1 where toTime64('00:00:01', 6) in (-inf));
-- Mixed sets and Nullable arguments take the same path.
select toDateTime64('1970-01-01 00:00:01.5', 1, 'UTC') in (1e30, 1.5);
select toNullable(toTime64('00:00:01.5', 3)) in (nan, 1.5);
-- The smallest tick of a `DateTime64(9)` is `Int64::min`, and `-9223372036.854776` scales to exactly that in a `Float64`.
-- `CAST` accepts it, so the `IN` path does too: the lower bound is inclusive, like the upper one.
select toDateTime64(-9223372036.854776, 9, 'UTC') in (-9223372036.854776);
select toDateTime64(-9223372036.854776, 9, 'UTC') in (-9223372036.854776, 1.5);
select count() from (select 1 where toDateTime64(-9223372036.854776, 9, 'UTC') in (-9223372036.86));
-- The `INSERT ... VALUES` expression fallback goes through `convertFieldToType` too: a `Float64` literal that is not parsed
-- by the streaming path materializes the same value as `CAST` (truncated to the scale, like `CAST(1.25 AS DateTime64(1))`).
drop table if exists t_05218_float;
create table t_05218_float (dt DateTime64(1, 'UTC'), t Time64(1)) engine = Memory;
insert into t_05218_float values (1.5 + 0, -1.5 + 0), (1.25 + 0, 1.25 + 0), (1735689600.0 + 0, -3599999.5 + 0);
select * from t_05218_float order by dt;
drop table t_05218_float;
-- The same boundary through the `VALUES` fallback: the minimum `DateTime64(9)` literal is stored, not rejected. The
-- streaming parser must not see the bare literal: as text, `-9223372036.854776` has more digits than the `Int64` ticks
-- hold and `readDateTime64Text` reports `DECIMAL_OVERFLOW` without trying the expression fallback; only its `Float64`
-- value is exactly `Int64::min`. The unary minus makes it an expression from the start.
drop table if exists t_05218_float_min;
create table t_05218_float_min (dt DateTime64(9, 'UTC')) engine = Memory;
insert into t_05218_float_min values (-(9223372036.854776));
select dt, dt = toDateTime64(-9223372036.854776, 9, 'UTC'), toInt64(toUnixTimestamp64Nano(dt)) from t_05218_float_min;
drop table t_05218_float_min;

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
select count() from t_05218 where dt in (toUInt128('99999999999999999999'));
select count() from t_05218 where dt in (toInt256(1), toUInt128('99999999999999999999'));
select count() from t_05218 where t in (toDecimal32('3599999', 0));
select count() from t_05218 where t in (toDecimal256('1.000000', 6), toDecimal128('-3600000', 0));
select count() from t_05218 where dt in (1.0);
select count() from t_05218 where dt in (1e30, 946684800.0);
select count() from t_05218 where t in (3599999.0, nan);
select count() from t_05218 where (dt, t) in ((1.0, 1.0), (946684800.0, 3600000.5));
drop table t_05218;

-- Overflowing scale conversion must report DECIMAL_OVERFLOW, not wrap.
select * from format(Values, 'x Time64(6)', '(253402207200000::Decimal64(0))'); -- { serverError DECIMAL_OVERFLOW }
select * from format(Values, 'x Time64(6)', '(-253402207200000::Decimal64(0))'); -- { serverError DECIMAL_OVERFLOW }
select * from format(Values, 'x Time64(9)', '(253402207200000::Decimal64(0))'); -- { serverError DECIMAL_OVERFLOW }
select * from format(Values, 'x Nullable(Time64(6))', '(253402207200000::Decimal64(0))'); -- { serverError DECIMAL_OVERFLOW }
select * from format(Values, 'x Array(Time64(6))', '([253402207200000::Decimal64(0)])'); -- { serverError DECIMAL_OVERFLOW }
select 1 where toTime64('00:00:01', 6) in (253402207200000::Decimal64(0)); -- { serverError DECIMAL_OVERFLOW }

-- Largest value whose rescale still fits Int64 is accepted; one tick more is rejected.
select * from format(Values, 'x Time64(6)', '(9223372036854::Decimal64(0))');
select * from format(Values, 'x Time64(6)', '(9223372036855::Decimal64(0))'); -- { serverError DECIMAL_OVERFLOW }

-- In-range conversions are unaffected, including negative times and the shrinking direction.
select * from format(Values, 'x Time64(3)', '(1::Decimal64(0))');
select * from format(Values, 'x Time64(6)', '(-12::Decimal64(0))');
select * from format(Values, 'x Time64(9)', '(-3599999::Decimal64(0))');
select * from format(Values, 'x Time64(0)', '(253402207200000::Decimal64(3))');
select * from format(Values, 'x Time64(0)', '(-253402207200000::Decimal64(3))');
select * from format(Values, 'x Time64(6)', '(1::Decimal64(6))');

-- The DateTime64 sibling branch keeps reporting the same overflow.
select 1 where toDateTime64('1970-01-01 00:00:01', 6) in (253402207200000::Decimal64(0)); -- { serverError DECIMAL_OVERFLOW }

-- A wrapped value must not be persisted. The conversion is asserted through `format` so that it
-- happens on the server: an inline `VALUES` list of an `INSERT` is parsed by the client, which
-- reports the same overflow as a client error, and which side parses it is not the contract here.
drop table if exists t_04883;
create table t_04883 (t Time64(6)) engine = MergeTree order by tuple();
insert into t_04883 select * from format(Values, 'x Time64(6)', '(253402207200000::Decimal64(0))'); -- { serverError DECIMAL_OVERFLOW }
insert into t_04883 values (-12::Decimal64(0));
select toString(t) from t_04883;
drop table t_04883;

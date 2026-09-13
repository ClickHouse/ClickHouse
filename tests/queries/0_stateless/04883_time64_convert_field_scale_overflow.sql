-- An overflowing scale conversion must never wrap. A reader that can fall back to a `CAST` converts the value
-- like every other numeric source of a `Time64` and saturates to the clock window of the type under the default
-- overflow mode; `convertFieldToType` alone has no mode to honour and keeps reporting `DECIMAL_OVERFLOW`.
select * from format(Values, 'x Time64(6)', '(253402207200000::Decimal64(0))');
select * from format(Values, 'x Time64(6)', '(-253402207200000::Decimal64(0))');
select * from format(Values, 'x Time64(9)', '(253402207200000::Decimal64(0))');
select * from format(Values, 'x Nullable(Time64(6))', '(253402207200000::Decimal64(0))');
select * from format(Values, 'x Array(Time64(6))', '([253402207200000::Decimal64(0)])');
select 1 where toTime64('00:00:01', 6) in (253402207200000::Decimal64(0)); -- { serverError DECIMAL_OVERFLOW }

-- A value whose rescale still fits the Int64 ticks is outside the clock window all the same, so it saturates
-- rather than being stored as a `Time64` that no clock reading can express.
select * from format(Values, 'x Time64(6)', '(9223372036854::Decimal64(0))');
select * from format(Values, 'x Time64(6)', '(9223372036855::Decimal64(0))');

-- In-range conversions are unaffected, including negative times and the shrinking direction.
select * from format(Values, 'x Time64(3)', '(1::Decimal64(0))');
select * from format(Values, 'x Time64(6)', '(-12::Decimal64(0))');
select * from format(Values, 'x Time64(9)', '(-3599999::Decimal64(0))');
select * from format(Values, 'x Time64(0)', '(253402207200000::Decimal64(3))');
select * from format(Values, 'x Time64(0)', '(-253402207200000::Decimal64(3))');
select * from format(Values, 'x Time64(6)', '(1::Decimal64(6))');

-- The DateTime64 sibling branch keeps reporting the same overflow.
select 1 where toDateTime64('1970-01-01 00:00:01', 6) in (253402207200000::Decimal64(0)); -- { serverError DECIMAL_OVERFLOW }

-- A wrapped value must not be persisted: the row that lands in the table is the saturated maximum, not the
-- `-999:59:59.722624` that the unguarded rescale produced. The conversion is asserted through `format` so that
-- it happens on the server: an inline `VALUES` list of an `INSERT` is parsed by the client, and which side
-- parses it is not the contract here. The two inserts make two parts, so the read is ordered.
drop table if exists t_04883;
create table t_04883 (t Time64(6)) engine = MergeTree order by tuple();
insert into t_04883 select * from format(Values, 'x Time64(6)', '(253402207200000::Decimal64(0))');
insert into t_04883 values (-12::Decimal64(0));
select toString(t) from t_04883 order by t;
drop table t_04883;

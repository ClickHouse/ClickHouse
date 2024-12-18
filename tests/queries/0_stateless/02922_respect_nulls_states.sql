SELECT toTypeName(first_value_respect_nullsState(dummy)), toTypeName(last_value_respect_nullsState(dummy)) from system.one;

SELECT first_value_respect_nullsMerge(t) FROM (Select first_value_respect_nullsState(dummy) as t FROM system.one);
SELECT first_value_respect_nullsMerge(t) FROM (Select first_value_respect_nullsState(NULL::Nullable(UInt8)) as t FROM system.one);
SELECT first_value_respect_nullsMerge(t) FROM (Select first_value_respect_nullsState(number) as t FROM numbers(5));
SELECT first_value_respect_nullsMerge(t) FROM (Select first_value_respect_nullsState(NULL::Nullable(UInt8)) as t FROM numbers(5));

SELECT last_value_respect_nullsMerge(t) FROM (Select last_value_respect_nullsState(dummy) as t FROM system.one);
SELECT last_value_respect_nullsMerge(t) FROM (Select last_value_respect_nullsState(NULL::Nullable(UInt8)) as t FROM system.one);
SELECT last_value_respect_nullsMerge(t) FROM (Select last_value_respect_nullsState(number) as t FROM numbers(5));
SELECT last_value_respect_nullsMerge(t) FROM (Select last_value_respect_nullsState(NULL::Nullable(UInt8)) as t FROM numbers(5));

SELECT first_value_respect_nullsMerge(t) FROM (Select first_valueState(number) as t from numbers(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT first_value_respect_nullsMerge(t) FROM (Select last_value_respect_nullsState(number) as t from numbers(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT last_value_respect_nullsMerge(t) FROM (Select first_value_respect_nullsState(number) as t from numbers(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT first_value_respect_nullsMerge(CAST(unhex('00'), 'AggregateFunction(any, UInt64)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Invalid (starts at 1)
SELECT first_value_respect_nullsMerge(CAST(unhex('00'), 'AggregateFunction(any_respect_nulls, UInt64)')); -- { serverError INCORRECT_DATA }
-- Not set (Default value)
SELECT first_value_respect_nullsMerge(CAST(unhex('01'), 'AggregateFunction(any_respect_nulls, UInt64)'));
SELECT finalizeAggregation(CAST(unhex('01'), 'AggregateFunction(any_respect_nulls, UInt64)'));
-- Set to NULL
SELECT first_value_respect_nullsMerge(CAST(unhex('02'), 'AggregateFunction(any_respect_nulls, UInt64)')); -- { serverError INCORRECT_DATA }
SELECT first_value_respect_nullsMerge(CAST(unhex('02'), 'AggregateFunction(any_respect_nulls, Nullable(UInt64))'));
SELECT finalizeAggregation(CAST(unhex('02'), 'AggregateFunction(any_respect_nulls, Nullable(UInt64))'));

-- Set to other value, but without providing value
SELECT first_value_respect_nullsMerge(CAST(unhex('03'), 'AggregateFunction(any_respect_nulls, UInt64)')); -- { serverError CANNOT_READ_ALL_DATA }

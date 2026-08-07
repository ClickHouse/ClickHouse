--- Regression test for src/AggregateFunctions/SingleValueData.cpp shouldUseFieldForValueData().
--- Types recursively containing Array are routed through the column-based
--- SingleValueDataGenericWithColumn instead of the Field-based SingleValueDataGeneric for
--- performance. Aggregate function states must not silently change encoding, and types
--- containing AggregateFunction must keep the legacy Field encoding: SerializationAggregateFunction
--- serializes a Field with a length prefix but a column without one, so switching a type that
--- contains AggregateFunction to the column-based state would make it unable to read states
--- written by older versions.

set session_timezone='UTC';

-- Array(Number): routed to the column-based state, pin the encoding.
select hex(minState([1,2,3]::Array(UInt64)));
select hex(maxState([1,2,3]::Array(UInt64)));
select hex(anyState([1,2,3]::Array(UInt64)));
select hex(anyLastState([1,2,3]::Array(UInt64)));
select hex(argMinState('x', [1,2,3]::Array(UInt64)));
select hex(argMaxState('x', [1,2,3]::Array(UInt64)));
select hex(argMinState([1,2,3]::Array(UInt64), 1));
select hex(argMaxState([1,2,3]::Array(UInt64), 1));

-- Array(String), also routed to the column-based state.
select hex(minState(['a','b']::Array(String)));

-- Array(Array(Number)): nested Array, still routed to the column-based state.
select hex(minState([[1,2],[3,4]]::Array(Array(UInt64))));

-- Tuple(Array(Number), Number): contains Array, routed to the column-based state.
select hex(minState(tuple([1,2,3]::Array(UInt64), 4::UInt64)));

-- Array(AggregateFunction) and Tuple(Array, AggregateFunction) must keep the legacy Field
-- encoding despite containing Array, because of the AggregateFunction serialization mismatch.
select hex(anyState([x])) from (select sumState(5::UInt64) as x);
select hex(anyState(tuple([1,2,3]::Array(UInt64), x))) from (select sumState(5::UInt64) as x);

-- Non-Array generic types: unaffected by the Array routing, still Field-based.
select hex(minState('59cd9014-8730-444c-95d0-40ed67c54268'::UUID));
select hex(minState(tuple(1::UInt64, 2::UInt64)));
select hex(minState(map('a', 1::UInt64)));

-- A hardcoded legacy Field-encoded state (Array(AggregateFunction)) must still decode correctly.
select sumMerge(elem) from (
    select arrayJoin(anyMerge(x)) as elem from (
        select cast(unhex('0101080500000000000000'), 'AggregateFunction(any, Array(AggregateFunction(sum, UInt64)))') as x
    )
);

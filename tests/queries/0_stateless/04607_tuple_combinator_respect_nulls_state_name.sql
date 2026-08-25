-- A -Tuple + RESPECT NULLS -State must be named after its resolved variant (any_respect_nullsTuple),
-- so its serialized type identifies the correct state layout on a round-trip.

SELECT toTypeName(anyTupleState(tuple(1::UInt32)) RESPECT NULLS);
SELECT toTypeName(anyTupleStateDistinct(tuple(1::UInt32)) RESPECT NULLS);
SELECT toTypeName(anyTupleState(tuple(1::UInt32)));
SELECT toTypeName(anyTupleState(tuple(1::UInt32)) RESPECT NULLS) = toTypeName(anyRespectNullsTupleState(tuple(1::UInt32)));

-- The window-variant state round-trips through its own (now correct) type name without confusion.
SELECT finalizeAggregation(CAST(anyTupleState(tuple(v)) RESPECT NULLS AS AggregateFunction(any_respect_nullsTuple, Tuple(Nullable(UInt32))))) FROM (SELECT 5::Nullable(UInt32) AS v);

-- Distributed round-trip (shard -> initiator re-resolution) over the full combinator chain no longer
-- reconstructs a mismatched state layout; the server survives and returns a result.
SELECT finalizeAggregation(anyTupleStateDistinct(tuple(number)) RESPECT NULLS) AS k FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY ALL WITH ROLLUP ORDER BY k;

-- The shared tuple name is the base aggregate name, not one element's instantiation. An only-null
-- element resolves to a `nothing` placeholder, so naming the tuple after it (countTuple((NULL, x)) ->
-- nothingUInt64Tuple) would drop the other element's real state on a round-trip. It must stay countTuple
-- regardless of which element is only-null.
SELECT toTypeName(countTupleState((NULL, number))) FROM numbers(3);
SELECT toTypeName(countTupleState((number, NULL))) FROM numbers(3);

-- Round-trip through the reported type name reconstructs both elements' state (no CANNOT_CONVERT_TYPE,
-- correct per-element counts): the NULL element counts nothing, the number element counts 3 rows.
SELECT finalizeAggregation(CAST(countTupleState((NULL, number)) AS AggregateFunction(countTuple, Tuple(Nullable(Nothing), UInt64)))) FROM numbers(3);

-- Multi-element RESPECT NULLS with an only-null element still resolves to the action-adjusted base name.
SELECT toTypeName(anyTupleState((NULL::Nullable(UInt32), number)) RESPECT NULLS) FROM numbers(3);

-- `-Map` deserialize used to `create` a nested state and then `emplace` it without checking for an
-- existing key. `emplace` does not insert over a key that is already there, so a repeated key in the
-- serialized state left the fresh state unreachable and undestroyed: a per-row leak of whatever the
-- nested state owned on the heap, driven entirely by the input bytes. `serialize` writes each key
-- once, so a repeated key means the state is malformed and is now rejected.

-- Two entries, both under key 1. Each holds 6 values, one past what `quantilesExact` keeps inline,
-- so the abandoned state owned a heap allocation.
SELECT finalizeAggregation(CAST(unhex(
    '02'
    || '01' || '06' || '000000000000000001000000000000000200000000000000030000000000000004000000000000000500000000000000'
    || '01' || '06' || '0a000000000000000b000000000000000c000000000000000d000000000000000e000000000000000f00000000000000'
    ) AS AggregateFunction(quantilesExactMap(0.5), Map(UInt8, UInt64)))); -- { serverError INCORRECT_DATA }

-- A well-formed state with the same two payloads under distinct keys still round-trips.
SELECT finalizeAggregation(CAST(unhex(
    '02'
    || '01' || '06' || '000000000000000001000000000000000200000000000000030000000000000004000000000000000500000000000000'
    || '02' || '06' || '0a000000000000000b000000000000000c000000000000000d000000000000000e000000000000000f00000000000000'
    ) AS AggregateFunction(quantilesExactMap(0.5), Map(UInt8, UInt64))));

-- And a state produced by ClickHouse itself is unaffected.
SELECT finalizeAggregation(quantilesExactMapState(0.5)(m))
FROM (SELECT map(toUInt8(number % 3), toUInt64(number)) AS m FROM numbers(30));

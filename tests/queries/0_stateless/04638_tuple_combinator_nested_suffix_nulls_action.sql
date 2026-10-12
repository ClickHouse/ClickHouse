-- When the -Tuple combinator wraps a nested name that itself carries combinator suffixes
-- (e.g. anyRespectNullsStateTuple nests anyRespectNullsState), a nulls-action modifier must be
-- applied at the base of the chain in the shared tuple state name, exactly as it is applied to the
-- instantiated elements. Otherwise the -State type name identifies a different state than the
-- elements actually hold, and a type-name round-trip (e.g. shard -> initiator in a distributed
-- query) reconstructs a mismatched function: CANNOT_CONVERT_TYPE at best, invalid memory access
-- in the state destructor at worst.

-- The shared name is adjusted at the base of the combinator chain.
SELECT toTypeName(anyRespectNullsStateTuple(tuple(toNullable(1))) IGNORE NULLS);
SELECT toTypeName(anyRespectNullsStateTuple(tuple(toNullable(1))) IGNORE NULLS) = toTypeName(anyStateTuple(tuple(toNullable(1))));
SELECT toTypeName(anyStateTuple(tuple(toNullable(1))) RESPECT NULLS);
SELECT toTypeName(anyRespectNullsStateTuple(tuple(toNullable(1))));

-- Distributed round-trip of the state type name: the initiator re-resolves the shard's state type
-- from its name and must reconstruct the same function.
SELECT anyRespectNullsStateTuple(tuple(toNullable(number))) IGNORE NULLS FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY ALL WITH TOTALS FORMAT Null;

-- The original fuzzed shape: a longer combinator chain under -OrDefault used to reach the state
-- destructor with a mismatched layout and abort the server.
SELECT anyRespectNullsStateDistinctArgMaxDistinctTupleOrDefault(tuple(toNullable(1)), tuple(toString(number) = '0')) IGNORE NULLS FROM remote('127.0.0.{1,2}', numbers(10)) GROUP BY ALL WITH TOTALS FORMAT Null;

-- A base function name whose tail merely looks like a combinator suffix is not stripped.
SELECT toTypeName(sumMap([1], [toNullable(1)]) IGNORE NULLS);

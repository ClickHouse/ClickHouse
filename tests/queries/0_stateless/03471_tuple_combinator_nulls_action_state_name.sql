-- Tags: shard

-- The -Tuple aggregate combinator must encode the nulls-action variant of its nested functions in
-- its serialized state type name. anyLastTuple(...) RESPECT NULLS resolves nested anyLast_respect_nulls
-- (a different state representation than plain anyLast), so the two must not share a type name.
SELECT toTypeName(anyLastTupleState(tuple(number)) RESPECT NULLS) != toTypeName(anyLastTupleState(tuple(number))) FROM numbers(1);
SELECT toTypeName(anyLastTupleState(tuple(number)) RESPECT NULLS) FROM numbers(1);
SELECT toTypeName(anyLastTupleState(tuple(number))) FROM numbers(1);

-- Placeholder-ness must be detected structurally, not by matching the bare nothing* names. An only-null
-- element under an inner combinator resolves to a composite placeholder (nothingUInt64Distinct here), so
-- an exact-name filter would let it win the base name and produce nothingUInt64DistinctTuple. The type
-- must stay sumDistinctTuple, and reconstructing from it must rebuild the second element as sumDistinct
-- (sum of distinct 0,1,2 = 3), not as the placeholder.
SELECT toTypeName(sumDistinctTupleState((NULL, toUInt64(number)))) FROM numbers(3);
SELECT finalizeAggregation(CAST(sumDistinctTupleState((NULL, toUInt64(number)))
    AS AggregateFunction(sumDistinctTuple, Tuple(Nullable(Nothing), UInt64)))) FROM numbers(3);

-- A Distributed table whose declared type differs from the shard type serializes shard aggregate
-- states under the declared type name. When the name did not encode the nulls-action variant, the
-- initiator reconstructed the wrong nested variant and reinterpreted the state bytes, crashing under
-- GROUP BY ALL WITH CUBE. It must now merge compatibly (or reject cleanly), never crash.
DROP TABLE IF EXISTS 03471_src;
DROP TABLE IF EXISTS 03471_dist;

CREATE TABLE 03471_src (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO 03471_src SELECT number FROM numbers(10);

CREATE TABLE 03471_dist (number UInt128)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), 03471_src, number);

SELECT last_valueTupleOrNull(tuple(
        tuple(* APPLY lambda(tuple(x), isNull(x)) EXCEPT '.*' REPLACE (2147483647 AS `e`)),
        tuple(* APPLY (lambda(tuple(x), isNull(x)), 'f_') EXCEPT '.*' REPLACE (1024 AS `e`))))
    RESPECT NULLS
FROM 03471_dist
GROUP BY ALL WITH CUBE;

DROP TABLE 03471_dist;
DROP TABLE 03471_src;

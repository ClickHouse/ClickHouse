-- Tags: shard

-- The -Tuple aggregate combinator must encode the nulls-action variant of its nested functions in
-- its serialized state type name. anyLastTuple(...) RESPECT NULLS resolves nested anyLast_respect_nulls
-- (a different state representation than plain anyLast), so the two must not share a type name.
SELECT toTypeName(anyLastTupleState(tuple(number)) RESPECT NULLS) != toTypeName(anyLastTupleState(tuple(number))) FROM numbers(1);
SELECT toTypeName(anyLastTupleState(tuple(number)) RESPECT NULLS) FROM numbers(1);
SELECT toTypeName(anyLastTupleState(tuple(number))) FROM numbers(1);

-- A placeholder element must be skipped in name derivation. The first element here is only-null, so
-- the factory collapses it to a bare nothing* placeholder; the name must still come from the real
-- second element. The type stays sumDistinctTuple and reconstructing from it rebuilds that element as
-- sumDistinct (sum of distinct 0,1,2 = 3), not the placeholder.
SELECT toTypeName(sumDistinctTupleState((NULL, toUInt64(number)))) FROM numbers(3);
SELECT finalizeAggregation(CAST(sumDistinctTupleState((NULL, toUInt64(number)))
    AS AggregateFunction(sumDistinctTuple, Tuple(Nullable(Nothing), UInt64)))) FROM numbers(3);

-- The stronger carrier: an only-null element whose placeholder keeps an outer combinator. sumArray
-- over [Nullable(Nothing)] resolves to the composite nothingNullArray (not one of the bare nothing*
-- names), so placeholder-ness must be detected structurally rather than by name. The type must stay
-- sumArrayTuple and the second element rebuilds as sumArray (sum of distinct 0,1,2 = 3).
SELECT toTypeName(sumArrayTupleState(tuple([NULL], [toUInt64(number)]))) FROM numbers(3);
SELECT finalizeAggregation(CAST(sumArrayTupleState(tuple([NULL], [toUInt64(number)]))
    AS AggregateFunction(sumArrayTuple, Tuple(Array(Nullable(Nothing)), Array(UInt64))))) FROM numbers(3);

-- An element that is itself an all-only-null -Tuple carries no nulls-action variant, so the outer
-- -Tuple must skip it (isOnlyNullPlaceholder must answer true for an all-placeholder -Tuple, not
-- only for the single-nested combinator chain). Otherwise the outer name takes the inner all-null
-- Tuple's pre-resolution spelling and the type stops being injective w.r.t. the nulls action, which
-- makes a -State round-trip fail with CANNOT_CONVERT_TYPE. IGNORE NULLS flips anyRespectNulls to any.
SELECT toTypeName(anyRespectNullsTupleIfTupleState(tuple(tuple(NULL), tuple(toNullable(number))), tuple(1, 1)) IGNORE NULLS) != toTypeName(anyRespectNullsTupleIfTupleState(tuple(tuple(NULL), tuple(toNullable(number))), tuple(1, 1))) FROM numbers(1);
SELECT toTypeName(anyRespectNullsTupleIfTupleState(tuple(tuple(NULL), tuple(toNullable(number))), tuple(1, 1)) IGNORE NULLS) FROM numbers(1);
SELECT toTypeName(anyRespectNullsTupleIfTupleState(tuple(tuple(NULL), tuple(toNullable(number))), tuple(1, 1))) FROM numbers(1);

-- A Distributed table whose declared type differs from the shard type serializes shard aggregate
-- states under the declared type name. When the name did not encode the nulls-action variant, the
-- initiator reconstructed the wrong nested variant and reinterpreted the state bytes, crashing under
-- GROUP BY ALL WITH CUBE. It must now merge compatibly (or reject cleanly), never crash.
DROP TABLE IF EXISTS 04627_src;
DROP TABLE IF EXISTS 04627_dist;

CREATE TABLE 04627_src (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO 04627_src SELECT number FROM numbers(10);

CREATE TABLE 04627_dist (number UInt128)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), 04627_src, number);

SELECT last_valueTupleOrNull(tuple(
        tuple(* APPLY lambda(tuple(x), isNull(x)) EXCEPT '.*' REPLACE (2147483647 AS `e`)),
        tuple(* APPLY (lambda(tuple(x), isNull(x)), 'f_') EXCEPT '.*' REPLACE (1024 AS `e`))))
    RESPECT NULLS
FROM 04627_dist
GROUP BY ALL WITH CUBE;

DROP TABLE 04627_dist;
DROP TABLE 04627_src;

-- The nested function may itself be combinator-wrapped, so the -Tuple name must encode the
-- action-adjusted variant of the RESOLVED nested function, not of the pre-resolution name string.
-- IGNORE NULLS flips the anyRespectNulls base back to any, so the nested anyRespectNullsArgMin
-- resolves to anyArgMin (a smaller state, different ArgMin key offset). The two variants must not
-- share a -Tuple type name.
SELECT toTypeName(anyRespectNullsArgMinTupleState(tuple(toNullable(number)), tuple(1.)) IGNORE NULLS) != toTypeName(anyRespectNullsArgMinTupleState(tuple(toNullable(number)), tuple(1.))) FROM numbers(1);
SELECT toTypeName(anyRespectNullsArgMinTupleState(tuple(toNullable(number)), tuple(1.)) IGNORE NULLS) FROM numbers(1);
SELECT toTypeName(anyRespectNullsArgMinTupleState(tuple(toNullable(number)), tuple(1.))) FROM numbers(1);

-- Distributed + WITH TOTALS reconstructs the aggregate from the action-less declared type name and
-- merges the totals row on the initiator. With a non-injective name the ArgMin key offset differed
-- between the resolved state and the reconstructed one, dereferencing a wild pointer while merging
-- SingleValueDataNumeric. It must now merge without crashing.
DROP TABLE IF EXISTS 04627_totals_src;
DROP TABLE IF EXISTS 04627_totals_dist;

CREATE TABLE 04627_totals_src (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO 04627_totals_src SELECT number FROM numbers(10);

CREATE TABLE 04627_totals_dist (number UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), 04627_totals_src);

SELECT anyRespectNullsArgMinTupleOrNull(tuple(toNullable(number)), tuple(1.)) IGNORE NULLS
FROM 04627_totals_dist
WITH TOTALS;

SELECT anyRespectNullsOrNullDistinctOrDefaultDistinctOrNullArgMinTupleOrNull(
        tuple((SELECT DISTINCT isNotNull(*) GROUP BY ALL)), tuple(1.)) IGNORE NULLS
FROM 04627_totals_dist
WITH TOTALS;

DROP TABLE 04627_totals_dist;
DROP TABLE 04627_totals_src;

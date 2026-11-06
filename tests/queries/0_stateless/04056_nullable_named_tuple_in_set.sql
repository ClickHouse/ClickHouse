-- Regression test: Nullable(Tuple) with named fields and IN with constant tuple.
-- The bug was that getSetElementsForConstantValue dropped tuple field names
-- when reconstructing the type after removing LowCardinality, causing a mismatch
-- between how CollectSets registered the set and how makeSetForInFunction looked it up.

SET allow_experimental_nullable_tuple_type = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_nullable_named_tuple;
CREATE TABLE t_nullable_named_tuple
(
    id UInt64,
    tup Nullable(Tuple(u LowCardinality(UInt64), s LowCardinality(String)))
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_nullable_named_tuple VALUES (1, (1, 'a')), (2, (2, 'b')), (3, NULL);

-- This query would cause "Logical error: No set is registered for key" before the fix,
-- because the set was registered with unnamed Tuple(UInt64, String) but looked up
-- with named Tuple(u UInt64, s String).
SELECT id FROM t_nullable_named_tuple WHERE tup NOT IN tuple(tuple(1, 'a')) ORDER BY id;

SELECT id FROM t_nullable_named_tuple WHERE tup IN tuple(tuple(2, 'b'), tuple(3, 'c')) ORDER BY id;

DROP TABLE t_nullable_named_tuple;

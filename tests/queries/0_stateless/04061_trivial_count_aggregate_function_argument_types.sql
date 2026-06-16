-- Verify that the optimized trivial count path produces correct argument types
-- in the AggregateFunction type. Previously, it used storage column types instead
-- of the aggregate function's actual argument types.

DROP TABLE IF EXISTS test_trivial_count_types;
CREATE TABLE test_trivial_count_types (v0 UInt32, v1 UInt64) ENGINE = MergeTree ORDER BY v0;
INSERT INTO test_trivial_count_types VALUES (1, 2);

SET optimize_trivial_count_query = 1;
SET allow_experimental_analyzer = 1;

-- count(v0 + v1) should have argument type UInt64 (the result of plus(UInt32, UInt64)),
-- not (UInt32, UInt64) (the types of the underlying columns).
SELECT trimBoth(explain) FROM (EXPLAIN header = 1 SELECT count(v0 + v1) FROM test_trivial_count_types)
WHERE explain LIKE '%Header:%' AND explain LIKE '%AggregateFunction%'
ORDER BY ALL;

DROP TABLE test_trivial_count_types;

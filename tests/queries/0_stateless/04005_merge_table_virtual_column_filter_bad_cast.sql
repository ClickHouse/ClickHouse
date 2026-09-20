-- Previously caused LOGICAL_ERROR: Bad cast from type DB::ColumnString to DB::ColumnLowCardinality
-- when filtering _table virtual column with tuple IN in `merge` table function.

SET allow_experimental_nullable_tuple_type = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_local_1;
CREATE TABLE test_local_1 (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO test_local_1 VALUES (1);

SELECT * FROM merge(currentDatabase(), 'test_local_1') WHERE ('test_local_1', 'test_local_2') IN (_table); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM merge(currentDatabase(), 'test_local_1') WHERE _table IN ('test_local_1', 'test_local_2');

DROP TABLE test_local_1;

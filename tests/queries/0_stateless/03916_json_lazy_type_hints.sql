-- Tags: no-fasttest, no-random-settings

SET enable_json_type = 1;
SET allow_suspicious_types_in_order_by = 1;
SET allow_experimental_json_lazy_type_hints = 1;

DROP TABLE IF EXISTS test_json_lazy;
CREATE TABLE test_json_lazy (j JSON) ENGINE = MergeTree ORDER BY tuple();

-- Insert data BEFORE adding type hints
INSERT INTO test_json_lazy VALUES ('{"a": 123, "b": "hello"}');
INSERT INTO test_json_lazy VALUES ('{"a": 456, "b": "world"}');

-- Query data before ALTER
SELECT 'Before ALTER:';
SELECT j.a, j.b FROM test_json_lazy ORDER BY j.a;

-- Add type hint (should be metadata-only, no mutation)
ALTER TABLE test_json_lazy MODIFY COLUMN j JSON(a UInt32);

-- Query data after ALTER (should still work)
SELECT 'After ALTER:';
SELECT j.a, j.b FROM test_json_lazy ORDER BY j.a;

-- Verify no mutation was created
SELECT 'Mutations count:';
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'test_json_lazy';

-- Insert new data after ALTER (should use type hint)
INSERT INTO test_json_lazy VALUES ('{"a": 789, "b": "test"}');

-- Query all data
SELECT 'All data:';
SELECT j.a, j.b FROM test_json_lazy ORDER BY j.a;

DROP TABLE test_json_lazy;

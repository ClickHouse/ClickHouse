-- Tags: no-fasttest, no-random-settings
-- Test: Verify that without lazy type hints setting, mutations are triggered

SET enable_json_type = 1;
SET allow_suspicious_types_in_order_by = 1;

-- Test: Without lazy type hints setting, ALTER should trigger mutation
DROP TABLE IF EXISTS test_json_no_lazy;
CREATE TABLE test_json_no_lazy (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_json_no_lazy VALUES (1, '{"val": 100}');
INSERT INTO test_json_no_lazy VALUES (2, '{"val": 200}');

-- Disable lazy type hints (default behavior)
SET allow_experimental_json_lazy_type_hints = 0;

-- This ALTER should trigger a mutation
ALTER TABLE test_json_no_lazy MODIFY COLUMN j JSON(val UInt32);

-- Wait for mutations to complete
SELECT sleepEachRow(0.1) FROM numbers(10) FORMAT Null;

-- Count mutations - should be 1 (mutation was triggered)
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'test_json_no_lazy' AND is_done = 1;

DROP TABLE test_json_no_lazy;

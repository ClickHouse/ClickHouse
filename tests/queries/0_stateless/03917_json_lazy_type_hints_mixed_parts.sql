-- Tags: no-fasttest, no-random-settings

SET enable_json_type = 1;
SET allow_suspicious_types_in_order_by = 1;
SET allow_experimental_json_lazy_type_hints = 1;

DROP TABLE IF EXISTS test_json_mixed;
CREATE TABLE test_json_mixed (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id;

-- Insert data in part 1 (before type hint)
INSERT INTO test_json_mixed VALUES (1, '{"x": 100, "y": "old"}');
INSERT INTO test_json_mixed VALUES (2, '{"x": 200, "y": "old"}');

-- Add type hint (metadata-only)
ALTER TABLE test_json_mixed MODIFY COLUMN j JSON(x UInt32);

-- Insert data in part 2 (after type hint)
INSERT INTO test_json_mixed VALUES (3, '{"x": 300, "y": "new"}');
INSERT INTO test_json_mixed VALUES (4, '{"x": 400, "y": "new"}');

-- Query all data (old parts + new parts should both work)
SELECT id, j.x, j.y FROM test_json_mixed ORDER BY id;

-- Verify no mutation
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'test_json_mixed';

DROP TABLE test_json_mixed;

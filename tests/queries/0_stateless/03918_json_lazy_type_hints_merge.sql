-- Tags: no-fasttest, no-random-settings

SET enable_json_type = 1;
SET allow_suspicious_types_in_order_by = 1;
SET allow_experimental_json_lazy_type_hints = 1;

DROP TABLE IF EXISTS test_json_merge;
CREATE TABLE test_json_merge (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id;

-- Insert data before type hint
INSERT INTO test_json_merge VALUES (1, '{"val": 100}');
INSERT INTO test_json_merge VALUES (2, '{"val": 200}');

-- Add type hint
ALTER TABLE test_json_merge MODIFY COLUMN j JSON(val UInt32);

-- Insert more data after type hint
INSERT INTO test_json_merge VALUES (3, '{"val": 300}');

-- Force merge
OPTIMIZE TABLE test_json_merge FINAL;

-- Query after merge
SELECT id, j.val FROM test_json_merge ORDER BY id;

DROP TABLE test_json_merge;

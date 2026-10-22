-- Validate that window PARTITION BY rejects Dynamic/Variant types
-- when allow_suspicious_types_in_group_by = 0, same as GROUP BY does.

SET allow_experimental_dynamic_type = 1;
SET allow_suspicious_types_in_group_by = 0;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS test_window_partition;
CREATE TABLE test_window_partition (d Dynamic, val UInt64) ENGINE = Memory;
INSERT INTO test_window_partition VALUES (1, 10), (2, 20), ('str', 30);

-- GROUP BY with Dynamic is blocked
SELECT d, sum(val) FROM test_window_partition GROUP BY d; -- { serverError ILLEGAL_COLUMN }

-- Window PARTITION BY with Dynamic must also be blocked
SELECT d, val, row_number() OVER (PARTITION BY d ORDER BY val) as rn FROM test_window_partition; -- { serverError ILLEGAL_COLUMN }

-- Multiple window functions with PARTITION BY Dynamic must also be blocked
SELECT d, val, sum(val) OVER (PARTITION BY d) as s FROM test_window_partition; -- { serverError ILLEGAL_COLUMN }

-- With allow_suspicious_types_in_group_by = 1, it should succeed
SET allow_suspicious_types_in_group_by = 1;
SELECT d, val, row_number() OVER (PARTITION BY d ORDER BY val) as rn FROM test_window_partition ORDER BY d, val;

DROP TABLE test_window_partition;

-- Test with Variant type
SET allow_suspicious_types_in_group_by = 0;
DROP TABLE IF EXISTS test_window_partition_variant;
CREATE TABLE test_window_partition_variant (v Variant(UInt64, String), val UInt64) ENGINE = Memory;
INSERT INTO test_window_partition_variant VALUES (1, 10), (2, 20), ('str', 30);

-- Window PARTITION BY with Variant must be blocked
SELECT v, val, row_number() OVER (PARTITION BY v ORDER BY val) as rn FROM test_window_partition_variant; -- { serverError ILLEGAL_COLUMN }

-- GROUP BY with Variant is also blocked
SELECT v, sum(val) FROM test_window_partition_variant GROUP BY v; -- { serverError ILLEGAL_COLUMN }

DROP TABLE test_window_partition_variant;

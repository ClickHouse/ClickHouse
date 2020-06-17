DROP TABLE IF EXISTS test;
CREATE TABLE test (x Int8, y Int8, z Int8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test VALUES (1, 2, 3), (1, 3, 3), (1, 4, 3), (1, 5, 3), (1, 2, 3);

SET optimize_monotonous_functions_in_order_by = 1;
SELECT * FROM test ORDER BY toFloat32(x), -y, -z DESC;
SELECT * FROM test ORDER BY toFloat32(x), -(-y), -z DESC;

SET optimize_monotonous_functions_in_order_by = 0;
SELECT * FROM test ORDER BY toFloat32(x), -y, -z DESC;
SELECT * FROM test ORDER BY toFloat32(x), -(-y), -z DESC;

DROP TABLE test;

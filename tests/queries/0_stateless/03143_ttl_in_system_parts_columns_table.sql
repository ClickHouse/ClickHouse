DROP TABLE IF EXISTS test_03143;

CREATE TABLE test_03143 (
   timestamp DateTime,
   x UInt32 TTL timestamp + INTERVAL 1 MONTH,
   y String TTL timestamp + INTERVAL 1 DAY,
   z String
)
ENGINE = MergeTree
ORDER BY tuple();


INSERT INTO test_03143 VALUES ('2100-01-01', 123, 'Hello, world!', 'xxx yyy');

SELECT
    name,
    column,
    type,
    column_ttl_min,
    column_ttl_max
FROM system.parts_columns
WHERE table = 'test_03143' and database = currentDatabase()
ORDER BY name, column;

DROP TABLE IF EXISTS test_03143;

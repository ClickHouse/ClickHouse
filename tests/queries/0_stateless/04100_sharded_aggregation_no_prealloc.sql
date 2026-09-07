SET enable_sharding_aggregator = 1;

DROP TABLE IF EXISTS test_no_prealloc_cube;
CREATE TABLE test_no_prealloc_cube (a String, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_no_prealloc_cube SELECT toString(number % 100), number FROM numbers(1000);
SELECT 'CUBE with materialize(NULL)';
SELECT a, count() FROM test_no_prealloc_cube GROUP BY a, materialize(NULL) WITH CUBE ORDER BY a LIMIT 5 SETTINGS enable_analyzer = 1;
DROP TABLE test_no_prealloc_cube;

DROP TABLE IF EXISTS test_arr;
CREATE TABLE test_arr (a String, b Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_arr VALUES ('x', [1,2]), ('y', [3]), ('x', [1,2]);
SELECT 'Multi-key with Array';
SELECT a, b, count() FROM test_arr GROUP BY a, b ORDER BY a, b;
DROP TABLE test_arr;

DROP TABLE IF EXISTS test_single_key_no_prealloc;
CREATE TABLE test_single_key_no_prealloc (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_single_key_no_prealloc VALUES ([1,2]), ([3]), ([1,2]);
SELECT 'Single Array key';
SELECT a, count() FROM test_single_key_no_prealloc GROUP BY a ORDER BY a;
DROP TABLE test_single_key_no_prealloc;

SELECT 'Nullable(UInt256) key (too large for nullable_keys256, falls back to nullable_serialized)';
SELECT toNullable(toUInt256(number)) AS k, count() FROM numbers(5) GROUP BY k ORDER BY k;

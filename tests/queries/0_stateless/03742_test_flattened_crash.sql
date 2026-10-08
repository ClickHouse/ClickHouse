DROP TABLE IF EXISTS test_flatten_nested_crash;
CREATE TABLE test_flatten_nested_crash
(
    `id` UInt64,
    `tenant` String,
    `arr.id` Array(Nullable(UInt64)),
    `arr.name` Array(Nullable(String)),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 8192;
INSERT INTO test_flatten_nested_crash
SELECT * FROM generateRandom(
    '`id` UInt64,
    `tenant` String,
    `arr.id` Array(Nullable(UInt64)),
    `arr.name` Array(Nullable(String)),
    `arr.nested` Array(Tuple(a String, b Float64))', 1, 10
) LIMIT 1;
ALTER TABLE test_flatten_nested_crash DROP COLUMN `arr.nested`;
ALTER TABLE test_flatten_nested_crash ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));
SELECT arr.nested FROM test_flatten_nested_crash ORDER BY arr.nested LIMIT 1;
DROP TABLE test_flatten_nested_crash;

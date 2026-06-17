-- Tags: no-fasttest
-- Reads a Wide-part column with nested JSON-in-JSON dynamic paths through the parallel
-- prefix-deserialization pool. Guards the nested-deserialization path that the pool drives.

SET enable_json_type = 1;

DROP TABLE IF EXISTS t_json_nested_pool;

CREATE TABLE t_json_nested_pool (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_json_nested_pool
SELECT number,
    toJSONString(map(
        'a', number,
        'nested', map('x', number * 2, 'y', map('deep', number * 3, 'deeper', number * 4)),
        concat('p', toString(number % 7)), number))::JSON
FROM numbers(2000);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_json_nested_pool' AND active AND part_type = 'Wide';

SET merge_tree_use_prefixes_deserialization_thread_pool = 1;
-- Keep the read local so it goes through the prefix-deserialization pool on this node.
SET enable_parallel_replicas = 0;
SELECT
    sum(data.a::UInt64),
    sum(data.nested.x::UInt64),
    sum(data.nested.y.deep::UInt64),
    sum(data.nested.y.deeper::UInt64)
FROM t_json_nested_pool;

SELECT data FROM t_json_nested_pool WHERE id = 5;

DROP TABLE t_json_nested_pool;

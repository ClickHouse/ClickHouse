-- Tags: no-fasttest
-- Regression test for a TSAN lock-order-inversion in nested JSON prefix deserialization.
-- The 'arr' path is an array of objects, so JSON stores it as a dynamic path of type
-- Array(JSON), i.e. a genuine nested SerializationObject. A full-column read of 'data' makes
-- the outer object's prefix-deserialization pool task recurse into the nested object's prefix.
-- The nested level still deserializes prefixes in parallel, but reuses the ancestor's already
-- thread-safe callbacks (settings.prefix_deserialization_callbacks_are_thread_safe) instead of
-- wrapping them under a second callbacks mutex - those two nested mutexes were the inversion.
-- A subcolumn read (data.arr) goes through SerializationObjectDynamicPath and reaches the
-- nested object as a standalone pool owner, so it does not exercise the re-entry; only a
-- full-column read does.

SET enable_json_type = 1;

DROP TABLE IF EXISTS t_json_nested_pool;

CREATE TABLE t_json_nested_pool (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- 'arr' is an array of objects, so JSON keeps it as a dynamic path of type Array(JSON)
-- (a nested SerializationObject) instead of flattening it to scalar dot-paths.
INSERT INTO t_json_nested_pool
SELECT number,
    toJSONString(map(
        'a', number,
        'arr', [map('x', number * 2, 'y', map('deep', number * 3)),
                map('x', number * 5, 'y', map('deep', number * 7))],
        concat('p', toString(number % 7)), number))::JSON
FROM numbers(2000);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_json_nested_pool' AND active AND part_type = 'Wide';

SET merge_tree_use_prefixes_deserialization_thread_pool = 1;
-- Keep the read local so it goes through the prefix-deserialization pool on this node.
SET enable_parallel_replicas = 0;

-- 'arr' is a nested Array(JSON), not a flattened scalar path.
SELECT startsWith(dynamicType(data.arr), 'Array(JSON') FROM t_json_nested_pool WHERE id = 5;

-- Full-column read: the outer object's pool task recurses into the nested 'arr' object's prefix.
SELECT data FROM t_json_nested_pool WHERE id = 5;

-- Read the whole 'data' value for every row so the recursion runs across all granules.
SELECT sum(length(toString(data))) FROM t_json_nested_pool;

DROP TABLE t_json_nested_pool;

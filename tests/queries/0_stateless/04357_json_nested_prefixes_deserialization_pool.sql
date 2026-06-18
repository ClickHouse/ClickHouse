-- Tags: no-fasttest
-- Reads a Wide-part JSON column that has a top-level dynamic path of type Array(JSON),
-- i.e. a genuine nested SerializationObject. Reading the array-of-objects subcolumns drives
-- a prefix-deserialization pool task to recurse into the nested object's prefix - exactly the
-- re-entrant path that must take the sequential branch (no nested callbacks_mutex).

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

-- The 'arr' path is a nested Array(JSON), not a flattened scalar path.
SELECT startsWith(dynamicType(data.arr), 'Array(JSON') FROM t_json_nested_pool WHERE id = 5;

-- Reading subcolumns of the array elements deserializes the nested object's prefixes through the pool.
SELECT arraySort(groupUniqArray(p)) FROM (SELECT arrayJoin(JSONDynamicPaths(arrayJoin(data.arr[]))) AS p FROM t_json_nested_pool);
SELECT sum(arraySum(arrayMap(e -> e.x::Int64, data.arr[]))) FROM t_json_nested_pool;
SELECT sum(arraySum(arrayMap(e -> e.`y.deep`::Int64, data.arr[]))) FROM t_json_nested_pool;

DROP TABLE t_json_nested_pool;

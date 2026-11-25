-- Tags: no-fasttest
-- Regression test for a TSAN lock-order-inversion in nested JSON prefix deserialization.
-- The 'arr' path is an array of objects, so JSON stores it as a dynamic path of type
-- Array(JSON), i.e. a genuine nested SerializationObject. A full-column read of 'data' makes
-- the outer object's prefix-deserialization pool task recurse into the nested object's prefix.
-- The nested level still deserializes prefixes in parallel, reusing the reader's callbacks as they
-- are: the reader synchronizes them itself, so no level wraps them under a callbacks mutex of its
-- own - two such nested mutexes, on recycled stack addresses, were the inversion.
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

-- Prefetching prefixes is what makes a nested object invoke prefixes_prefetch_callback off the
-- reader thread: the outer level runs its own prefetch pass before it schedules any task, so the
-- nested pass, and that callback with it, runs inside the pool task owning the path. Own table, read
-- exactly once, because a column the queries above have already read comes from the uncompressed
-- cache and is then never prefetched at all.
DROP TABLE IF EXISTS t_json_nested_prefetch;

-- The layout settings keep the substream a file of its own, named after the path: the runner
-- randomizes packed part storage and long file name hashing, and either one leaves the prefetches
-- log naming something the assertion below cannot attribute. Same reason for the disk, whose object
-- storage keys carry no path at all.
CREATE TABLE t_json_nested_prefetch (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, disk = 'default',
         min_bytes_for_full_part_storage = 0, replace_long_file_name_to_hash = 0;

-- Two array-of-object paths, so the two nested prefix passes can overlap.
INSERT INTO t_json_nested_prefetch
SELECT number,
    toJSONString(map(
        'arr', [map('x', number, 'y', map('deep', number * 3))],
        'arr_b', [map('x', number * 5, 'y', map('deep', number * 7))],
        concat('p', toString(number % 7)), number))::JSON
FROM numbers(500);

-- The read method is pinned because a synchronous one turns ReadBuffer::prefetch into a no-op.
SELECT sum(length(toString(data))) FROM t_json_nested_prefetch
SETTINGS local_filesystem_read_prefetch = 1, remote_filesystem_read_prefetch = 1,
         local_filesystem_read_method = 'pread_threadpool',
         remote_filesystem_read_method = 'threadpool',
         enable_filesystem_read_prefetches_log = 1, log_comment = 'nested_prefix_prefetch';

SYSTEM FLUSH LOGS query_log, filesystem_read_prefetches_log;

-- A substream that exists only below a nested object was prefetched, so the callback ran at the
-- nested level and not just in the outer pass. Presence, not a count: how many prefetches one read
-- submits depends on granularity and on the reader's own settings.
SELECT count() > 0 FROM system.filesystem_read_prefetches_log
WHERE query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
          AND log_comment = 'nested_prefix_prefetch')
  AND path LIKE '%.x.dynamic_structure%';

-- Asserted after the read above, which has to be the first one: both paths were nested objects,
-- so that read did recurse.
SELECT startsWith(dynamicType(data.arr), 'Array(JSON'), startsWith(dynamicType(data.arr_b), 'Array(JSON')
FROM t_json_nested_prefetch WHERE id = 5;

DROP TABLE t_json_nested_prefetch;

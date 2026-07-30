-- Regression coverage for the `MAP` shared-data sub-object read over the DeserializationPrefixesCache.
-- `DeserializeBinaryBulkStateSubObjectSharedData::clone` used to copy `map_state` by pointer, so the prefix
-- cache handed every reader a clone that shared the same nested `map_state` with the cached original. The
-- `MAP` sub-object read path mutates that state via `serialization_map->deserializeBinaryBulkWithMultipleStreams`,
-- so a prefix-cached read of an existing / compatibility `object_shared_data_serialization_version = 'map'`
-- part could reuse an already-advanced state or race across concurrent readers. `map_state` must be deep-cloned
-- like `bucket_map_states`. See https://github.com/ClickHouse/ClickHouse/issues/105626.

DROP TABLE IF EXISTS t_json_map_prefix_cache_wide;

CREATE TABLE t_json_map_prefix_cache_wide
(
    id UInt64,
    json JSON(max_dynamic_paths = 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 128,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'map',
    object_shared_data_serialization_version_for_zero_level_parts = 'map';

-- Five distinct paths per row with only two dynamic slots, so at least one `a.*` path (read by `json.^a`)
-- always lands in shared data and goes through the `MAP` sub-object read path. A small `index_granularity`
-- gives many granules so several threads read the same part concurrently through the shared prefix state.
INSERT INTO t_json_map_prefix_cache_wide
SELECT
    number,
    toJSONString(map(
        'a.k1', number % 5,
        'a.k2', number % 7,
        'a.k3', number % 13,
        'b', number % 3,
        'c', number % 11))
FROM numbers(4000);

-- All active parts must be Wide so the local read uses the deserialization prefixes cache.
SELECT count() > 0, countIf(part_type = 'Wide') = count()
FROM system.parts
WHERE database = currentDatabase() AND table = 't_json_map_prefix_cache_wide' AND active;

-- The concurrent, prefix-cached sub-object read must match the single-threaded read that does not use the
-- cache. With the shallow clone, concurrent readers mutate a shared `map_state` (a data race that also
-- corrupts the result), while a sequential re-clone starts from an already-advanced state.
WITH
(
    SELECT sum(cityHash64(toString(json.^a)))
    FROM t_json_map_prefix_cache_wide
    SETTINGS max_threads = 1, merge_tree_use_deserialization_prefixes_cache = 0
) AS ground_truth
SELECT
    ground_truth = (
        SELECT sum(cityHash64(toString(json.^a)))
        FROM t_json_map_prefix_cache_wide
        SETTINGS
            max_threads = 8,
            merge_tree_use_deserialization_prefixes_cache = 1,
            merge_tree_min_rows_for_concurrent_read = 1,
            merge_tree_min_bytes_for_concurrent_read = 1,
            max_block_size = 256
    ),
    ground_truth = (
        SELECT sum(cityHash64(toString(json.^a)))
        FROM t_json_map_prefix_cache_wide
        SETTINGS max_threads = 4, merge_tree_use_deserialization_prefixes_cache = 1
    );

-- Reading the whole column together with the sub-object shares the same prefix cache too.
SELECT count(), uniqExact(toString(json.^a))
FROM t_json_map_prefix_cache_wide
SETTINGS max_threads = 8, merge_tree_use_deserialization_prefixes_cache = 1;

DROP TABLE t_json_map_prefix_cache_wide;

-- The same path is reachable for a JSON nested inside a Tuple.
DROP TABLE IF EXISTS t_json_map_prefix_cache_wide_tuple;

CREATE TABLE t_json_map_prefix_cache_wide_tuple
(
    id UInt64,
    t Tuple(data JSON(max_dynamic_paths = 2))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 128,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'map',
    object_shared_data_serialization_version_for_zero_level_parts = 'map';

INSERT INTO t_json_map_prefix_cache_wide_tuple
SELECT
    number,
    tuple(toJSONString(map(
        'a.k1', number % 5,
        'a.k2', number % 7,
        'a.k3', number % 13,
        'b', number % 3,
        'c', number % 11)))
FROM numbers(4000);

WITH
(
    SELECT sum(cityHash64(toString(t.data.^a)))
    FROM t_json_map_prefix_cache_wide_tuple
    SETTINGS max_threads = 1, merge_tree_use_deserialization_prefixes_cache = 0
) AS ground_truth
SELECT
    ground_truth = (
        SELECT sum(cityHash64(toString(t.data.^a)))
        FROM t_json_map_prefix_cache_wide_tuple
        SETTINGS
            max_threads = 8,
            merge_tree_use_deserialization_prefixes_cache = 1,
            merge_tree_min_rows_for_concurrent_read = 1,
            merge_tree_min_bytes_for_concurrent_read = 1,
            max_block_size = 256
    );

DROP TABLE t_json_map_prefix_cache_wide_tuple;

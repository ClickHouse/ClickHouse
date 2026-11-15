-- Tags: no-random-settings, no-random-merge-tree-settings
-- The `IN` subquery of a `TTL` expression asks for parallel replicas in its own `SETTINGS` clause. The analyzer
-- re-applies that clause, so without stripping it the set is built off the merge thread and the counter reads 0.
-- Settings are pinned: `SetsBuiltFromSubquery` reaches `part_log` only while the set is built on the merge thread.

SET optimize_throw_if_noop = 0;
SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_ttl_pr_set;
DROP TABLE IF EXISTS t_ttl_pr_set_keys;

CREATE TABLE t_ttl_pr_set_keys (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ttl_pr_set_keys SELECT number * 2 FROM numbers(50);

CREATE TABLE t_ttl_pr_set
(
    id UInt64,
    ver UInt64,
    d DateTime,
    c1 UInt64,
    c2 UInt64
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY id
TTL d + INTERVAL 1 DAY WHERE id IN (SELECT id FROM {CLICKHOUSE_DATABASE:Identifier}.t_ttl_pr_set_keys SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1)
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

-- Two versions of keys 0..99 in one part, all expired; the even keys match the TTL `WHERE` and are
-- deleted. The first merge of that part applies the TTL, whether a background merge or the
-- `OPTIMIZE` runs it; `OPTIMIZE ... FINAL` waits for a running merge.
INSERT INTO t_ttl_pr_set
SELECT number % 100, intDiv(number, 100) + 1 AS ver, '2000-01-01 00:00:00', ver, ver FROM numbers(200);

OPTIMIZE TABLE t_ttl_pr_set FINAL;

SYSTEM FLUSH LOGS part_log;

-- `SetsBuiltFromSubquery` counts sets filled by running their subquery, not ones taken from the cache.
SELECT merge_algorithm, ProfileEvents['SetsBuiltFromSubquery']
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_ttl_pr_set' AND event_type = 'MergeParts' AND merged_from = ['all_1_1_0'];

DROP TABLE t_ttl_pr_set;
DROP TABLE t_ttl_pr_set_keys;

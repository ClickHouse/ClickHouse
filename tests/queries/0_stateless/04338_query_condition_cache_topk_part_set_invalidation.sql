-- Tags: long, no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag long: needs ~1.5M rows across two partitions for the TopK threshold to drop
--   whole granules of the unchanged part, so on the slower S3 + sanitizer
--   configuration a single run takes ~180s and crosses the flaky-check "test runs
--   too long" threshold; the data volume is structurally required by the
--   part-set-invalidation scenario this test checks.
--
-- Correctness regression for the Query Condition Cache (QCC) on the
-- `ORDER BY <column> LIMIT n` (TopK) plan: a granule-skip decision recorded for
-- one part depends on the TopK threshold, which is computed from rows of *all*
-- parts the query reads. The QCC key is `(table_uuid, part_name, condition_hash)`,
-- so when another part is dropped or mutated, the entry of an *unchanged* part
-- still matches the key while the decision it stores is stale. The TopK salt
-- therefore includes a hash of the whole part-set snapshot
-- (`ReadFromMergeTree::setTopKColumn`); this test checks that changing the part
-- set really invalidates the cached decisions.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
-- Keep the `WHERE` predicate as a separate `FilterStep` above `ReadFromMergeTree`
-- so the dynamic-filtering branch of `tryOptimizeTopK` applies and the WHERE
-- condition is the one written into the query condition cache.
SET optimize_move_to_prewhere = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
-- Deterministic read so the warm run reliably populates the cache: single thread,
-- pinned chunk size, no random mark-range splitting (see
-- `04320_query_condition_cache_topk_correctness` for the full rationale).
SET max_threads = 1;
SET max_block_size = 8192;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (p UInt8, id UInt32, k UInt32, w UInt32) ENGINE = MergeTree
PARTITION BY p ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

-- Two partitions = two parts, and partition 0 sorts first in the read order.
--
-- Partition 0 holds the `w = 7` rows with the largest `k` (`k` in
-- [500000, 1000000)), so the warm run reads it first and the running threshold
-- of `ORDER BY k DESC LIMIT 5` settles at `k = 995007`.
--
-- Partition 1 interleaves two kinds of rows in every granule (`id` is a per-row
-- hash, so they are scattered uniformly):
--   * "victims" (even `number`): `k` in [0, 500000), `w` matching every 1000th
--     row — each `k` is *below* the threshold, so `__topKFilter` drops them
--     before the `WHERE` filter ever sees them;
--   * "decoys" (odd `number`): `k` >= 1500000 but `w = 1` — they survive
--     `__topKFilter` at any threshold, so every chunk of partition 1 reaches the
--     `WHERE` filter non-empty, and `w = 7` then reduces it to zero rows.
-- Net effect: the warm run records *all* granules of partition 1 as skippable
-- under the TopK-salted WHERE key, although they contain `w = 7` rows — those
-- were merely below a threshold that partition 0's rows established.
INSERT INTO tab
SELECT 0, intHash32(number), number, number % 1000
FROM numbers(500000, 500000)
SETTINGS max_insert_threads = 1, max_insert_block_size = 2_000_000, min_insert_block_size_rows = 2_000_000;

INSERT INTO tab
SELECT 1,
       intHash32(number),
       if(number % 2 = 0, intDiv(number, 2), 1500000 + number),
       if(number % 2 = 0, intDiv(number, 2) % 1000, 1)
FROM numbers(1_000_000)
SETTINGS max_insert_threads = 1, max_insert_block_size = 2_000_000, min_insert_block_size_rows = 2_000_000;

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT '--- DESC LIMIT 5 ground truth (QCC off): top rows all come from partition 0';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5 SETTINGS use_query_condition_cache = 0;

SELECT '--- Warm run records partition 1 granules as skippable under the TopK salt';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5;
SELECT count() > 0 FROM system.query_condition_cache;

SELECT '--- Cached run, same part set: must match ground truth';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5;

-- Dropping partition 0 removes the rows the threshold was computed from. The new
-- top-5 lives entirely in partition 1, whose part is unchanged (same name) and
-- whose granules the warm run recorded as skippable. Reusing those entries would
-- return an empty result; the part-set hash in the TopK salt must invalidate them.
ALTER TABLE tab DROP PARTITION 0;

SELECT '--- After DROP PARTITION: ground truth (QCC off) comes from partition 1';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5 SETTINGS use_query_condition_cache = 0;
SELECT '--- QCC on must not reuse granule decisions recorded under the old part set';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5;

DROP TABLE tab;
